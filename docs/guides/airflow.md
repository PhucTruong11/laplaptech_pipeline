# Chạy và hiểu DAG Airflow

## 1. Điều kiện và vai trò

Stack đã được dựng theo [hướng dẫn Docker](docker.md), Docker Desktop chạy Linux containers và sáu service thường trực healthy. Bài này dùng baseline **Airflow 3.3.2 / Python 3.13 / LocalExecutor**, Docker provider **4.6.1** được pin trong source.

| Thành phần | Việc thực hiện |
|---|---|
| API server | UI tại [localhost:8080](http://localhost:8080), API và execution API cho task |
| DAG processor | Import source DAG và lưu định nghĩa vào metadata |
| Scheduler + LocalExecutor | Lập lịch, chạy Airflow task process và gọi Docker Engine |
| Metadata PostgreSQL | Giữ user, DAG/run/task state, Connections/Variables; tách khỏi warehouse |
| DockerOperator | Tạo runtime container, thu stdout/stderr và chờ exit code |
| Runtime Python/dbt | Đọc ClickHouse, nạp raw, kiểm tra và biến đổi dữ liệu trong warehouse |

Airflow quản lý thứ tự và trạng thái task; dependency SQL giữa 31 model vẫn do dbt quản lý. Xem [dbt](dbt.md) để chạy/chọn model và [data flow](../architecture/data_flow.md) để hiểu grain/metric.

## 2. Source, workspace và cấu hình

Source chuẩn là [`infra/laplaptech/dags/laplaptech_pipeline.py`](../../infra/laplaptech/dags/laplaptech_pipeline.py). Compose mount thư mục DAG này read-only vào `/opt/airflow/dags/laplaptech` của API, scheduler và DAG processor qua `LAPLAPTECH_PROJECT_DIR`.

| Nơi làm việc | Nội dung |
|---|---|
| Root repo `laplaptech_pipeline` | Sửa DAG, ingestion/dbt, Dockerfiles, tests và tài liệu |
| Workspace sibling `airflow-platform` | Compose vận hành, `.env` Airflow keys, smoke DAG và backups |

Không copy thêm một file Python cùng DAG ID vào workspace platform. Sửa DAG chỉ cần lưu source rồi đợi processor parse; sửa code ingestion/dbt cần rebuild runtime image.

Các lệnh dưới đây chạy tại **workspace platform**. Từ root repo có thể chuyển vào workspace mặc định:

```powershell
$platformDir = Join-Path (Split-Path -Path (Get-Location).Path -Parent) 'airflow-platform'
Set-Location -LiteralPath $platformDir
```

Nếu setup đã chọn workspace khác, dùng đúng đường dẫn đó. Luôn truyền đủ `compose.yaml` và `compose.laplaptech.yaml`.

## 3. Parse và chạy pipeline

Kiểm tra services, version và DAG import:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml ps
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec -T airflow-scheduler airflow version
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec -T airflow-dag-processor airflow dags list-import-errors
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec -T airflow-scheduler airflow dags list
```

**Checkpoint:** version 3.3.2, không có import error và thấy `laplaptech_pipeline`. UI mở được chưa chứng minh task đã chạy. Đăng nhập bằng `AIRFLOW_ADMIN_USERNAME`/`AIRFLOW_ADMIN_PASSWORD` trong `.env` **platform**, không dùng placeholder `.env.example`.

Trong UI: mở DAG → unpause → Trigger. Lần rebuild Silver từ raw, chọn parameter `full_refresh=true`; các lượt thường để `false`.

CLI tương đương cho một lượt thường, tại workspace platform:

```powershell
$runId = 'manual-' + (Get-Date -Format 'yyyyMMddTHHmmss')
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec -T airflow-scheduler airflow dags unpause laplaptech_pipeline
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec -T airflow-scheduler airflow dags trigger --run-id $runId laplaptech_pipeline
```

Mỗi run ID phải khác nhau. Unpause lần đầu có thể tạo lượt lịch mới nhất đã đến hạn; các run vẫn chạy tuần tự. Trigger manual không tự thay param mặc định của các lượt scheduled.

## 4. Đọc DAG theo ý nghĩa vận hành

```text
ingest → freshness → build → reconcile
```

| Task | Công việc | Timeout / retry |
|---|---|---|
| `ingest` | 5 snapshot atomic theo bảng + event dedup/upsert | 45 phút; retry 1 lần sau 2 phút |
| `freshness` | `dbt source freshness` | 5 phút; không retry; warn 24 giờ/error 48 giờ |
| `build` | `dbt build`, gồm model và data tests | 30 phút; retry 1 lần sau 2 phút |
| `reconcile` | Inventory, duplicate ID, raw/Bronze/Silver và daily totals | 10 phút; không retry |

Lịch là **08:15 và 16:45**, timezone `Asia/Ho_Chi_Minh`, với hai cron `15 8 * * *` và `45 16 * * *`. `catchup=False` không tạo mọi lượt bị lỡ và không thay bounded backfill. Khi Docker tắt, scheduler dừng; GitHub CI không chạy ETL thay nó.

`max_active_runs=1`, `max_active_tasks=1` và pool `laplaptech_warehouse=1` giới hạn writer của DAG; toàn run có timeout 2 giờ. Các cơ chế này không chặn người tự chạy CLI ngoài Airflow trên cùng warehouse.

Factory `pipeline_stage` dùng cùng image `laplaptech-runtime:local-v1`, UID 10001 và giới hạn RAM 2 GB mỗi task. Config volume chỉ đọc; artifact volume cho phép ghi. `mount_tmp_dir=False` tránh cố mount đường dẫn tạm của scheduler vào sibling container. Dataset không truyền qua XCom.

`full_refresh=true` chỉ thêm `--full-refresh` vào **dbt build**; nó rebuild incremental Silver từ raw hiện có, không kéo lại toàn bộ version lịch sử ClickHouse. Ingestion vẫn theo snapshot/lookback của loader.

Nếu ingest/freshness fail, build bị chặn; build fail thì reconcile không chạy. Retry/Clear không rollback những bảng đã commit. Gold chưa publish chung một transaction nên build fail giữa chừng có thể để lại các mart khác phiên bản.

## 5. Assets, logs và artifacts

| Asset | Được cập nhật khi |
|---|---|
| `LaplapTech raw` | Task ingest success |
| `LaplapTech validated Gold` | Task reconcile success |

Assets ghi metadata dữ liệu sẵn sàng, không chứa dataset. DAG hiện vẫn dùng lịch cron; chưa có consumer DAG nào được lập lịch bằng hai Assets này.

Trong UI, mở đúng **run → task → Logs**; xem try number để phân biệt attempt. Docker task container tự xóa khi kết thúc, nhưng Airflow logs và runtime artifacts vẫn nằm trên named volumes.

Mỗi stage lưu `/artifacts/<sha256(run_id)[:20]>/<stage>/attempt-<try>-<uuid>/report.json`. Build có `target/manifest.json` và `target/run_results.json`; freshness có `target/sources.json`. Nếu Docker chưa tạo được container thì chưa có stage report; nếu dbt chưa chạy thì chưa có dbt artifacts.

Kiểm tra log control plane tại workspace platform:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml logs --tail 80 airflow-scheduler airflow-dag-processor airflow-api-server
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec -T airflow-scheduler airflow pools list
```

**Checkpoint:** bốn task success, artifact của đúng run ghi đủ 31 model/47 data tests đạt và reconcile khớp. Cách nghiệm thu lại bằng verifier có trong [testing](testing.md); các report theo ngày nằm ở [validation](../validation/README.md).

## 6. Sửa DAG hoặc runtime an toàn

Sửa DAG tại source trong repo rồi kiểm tra lại import errors. Tránh gọi mạng/truy vấn DB ở top-level file DAG vì processor import lặp lại.

Khi thay runtime/config, cách khuyến nghị là chạy lại bootstrap từ **root repo**:

```powershell
.\scripts\Start-LocalStack.ps1
```

Bootstrap tạm pause riêng DAG LaplapTech trước active-run check, giữ pause suốt build/nạp config, rồi khôi phục trạng thái cũ khi thành công. Nếu còn active run, nó dừng trước cập nhật. Nếu lỗi sau khi bắt đầu cập nhật, DAG giữ paused để sửa lỗi trước khi unpause. Xem [Docker](docker.md) cho rebuild, partial-stack recovery và giữ keys/volumes.

Nếu thao tác thủ công, pause DAG, đợi hết queued/running runs, rồi mới thay runtime/config; chỉ unpause sau khi kiểm chứng. Không đồng thời chạy ingestion ngoài Airflow; [ingestion](ingestion.md) mô tả recovery/backfill có kiểm soát.

## 7. Bài tập smoke DAG và xử lý lỗi

`platform_smoke_test` kiểm tra control plane bằng `prepare_payload → validate_payload`, schedule manual, retry 1 lần sau 15 giây. Nó truyền dict nhỏ qua XCom và không truy cập warehouse.

1. Trigger smoke trong UI, đợi cả hai task success.
2. Mở Logs của validate, tìm `Smoke test passed: project=airflow-platform, total=10`.
3. Xem XCom của prepare để hiểu dữ liệu nhỏ giữa hai task.
4. Khi không có smoke run active, thử đổi điều kiện `total != 10` thành `total != 11` trong smoke file ở workspace platform; đợi parse rồi trigger để quan sát retry/fail.
5. Khôi phục điều kiện cũ, đợi parse, chạy lại; có thể dùng Clear để chạy lại task và so sánh các attempt.

Đây là bài tập để tự thực hành; không suy ra đã thử failure/recovery chỉ từ một report success.

| Triệu chứng | Nơi kiểm tra |
|---|---|
| DAG không xuất hiện/import error | Source mount, `LAPLAPTECH_PROJECT_DIR`, log processor và imports |
| Task queued lâu | Scheduler health, pool, run trước và trạng thái paused |
| Freshness error | `sources.json` và timestamp nguồn; không bỏ kiểm tra để làm run xanh |
| Exit 137 | RAM/task log; 2 GB là giới hạn task, không phải tổng RAM stack |
| Build/reconcile lỗi | `run_results.json`, stage report, DB target và [dbt guide](dbt.md) |

Nâng Airflow cần pin core/provider/Python tương thích, pause/đợi active runs, backup metadata cùng `.env`, thử migration và restore trên môi trường riêng trước. Không chỉ đổi tag trên metadata đã migrate. Tham khảo [upgrade guide baseline 3.3.2](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/upgrading.html).
