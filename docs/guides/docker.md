# Docker: cài đặt và vận hành toàn hệ thống

## 1. Mục đích và cấu hình

ClickHouse public cung cấp dữ liệu; Airflow tạo task container để ingest → freshness → dbt build → reconcile; PostgreSQL local giữ warehouse; Streamlit đọc Gold. GitHub Actions chỉ chạy CI fixture. Thiết kế và lệnh dưới đây đối chiếu source hiện tại; bằng chứng cutover ngày 20/10/2026 nằm trong [report](../validation/local_cutover_2026-10-20.json), không phải trạng thái realtime.

### Lệnh nhanh để lưu vào Notepad (máy hiện tại)

Chạy trong PowerShell. Các đường dẫn dưới đây là của máy người duy trì; nếu clone sang máy khác, thay `Set-Location` bằng đường dẫn của bạn. Trước khi tắt Airflow/LapLapTech, đợi mọi DAG run/task đang chạy kết thúc; công tắc `.\scripts\Set-LocalStack.ps1 -State Off` ở mục 5 tự kiểm tra điều này. Lệnh Compose `stop` chỉ dừng container, không xóa database trong named volume.

```text
LapLapTech
- Check toàn bộ stack, kể cả container đang tắt
Set-Location 'D:\Dev\Project\airflow-platform'
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard ps -a

- Off toàn bộ stack Docker Airflow/LapLapTech
Set-Location 'D:\Dev\Project\airflow-platform'
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard stop -t 60

- On Airflow và warehouse LapLapTech; dashboard vẫn tắt
Set-Location 'D:\Dev\Project\airflow-platform'
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml up -d

- On dashboard sau khi đã On stack Airflow/LapLapTech và dừng BrewLite DB
Set-Location 'D:\Dev\Project\airflow-platform'
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard up -d dashboard

- Off riêng dashboard
Set-Location 'D:\Dev\Project\airflow-platform'
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard stop dashboard

BrewLite
- Check BrewLite DB
Set-Location 'D:\Dev\Subject_Uni\SWE\brewlite'
docker compose -f .\docker\docker-compose.dev.yml ps

- Off BrewLite DB
Set-Location 'D:\Dev\Subject_Uni\SWE\brewlite'
docker compose -f .\docker\docker-compose.dev.yml stop db

- On BrewLite DB
Set-Location 'D:\Dev\Subject_Uni\SWE\brewlite'
docker compose -f .\docker\docker-compose.dev.yml up -d db
```

Airflow là hạ tầng dùng chung: `Off toàn bộ stack` cũng dừng lịch của những project khác khi chúng được nối vào sau này. BrewLite là Compose riêng và hiện chỉ đóng gói PostgreSQL; lệnh BrewLite không bật/tắt web app nếu bạn chạy app bằng terminal riêng. Với ngân sách RAM 8 GiB hiện tại, không bật đồng thời BrewLite DB và dashboard LapLapTech.

### Thành phần và cổng kết nối

| Thành phần | Trên máy host | Trong Docker |
|---|---|---|
| Airflow UI | [localhost:8080](http://localhost:8080) | API server, scheduler, DAG processor |
| Dashboard, bật khi cần | [localhost:8501](http://localhost:8501) | `dashboard:8501` |
| Warehouse | `127.0.0.1:55434` | `warehouse-laplaptech:5432` |
| Metadata Airflow | Không publish | `metadata:5432/airflow_meta` |

Đăng nhập warehouse từ DataGrip/DBeaver/pgAdmin:

| Trường | Giá trị mặc định |
|---|---|
| Host | `127.0.0.1` |
| Port | `55434` |
| Database | `laplaptech_pipeline_dev` |
| User | `laplaptech` |
| Password local | `laplaptech_lab_only` |
| SSL mode | `prefer` |

```text
jdbc:postgresql://127.0.0.1:55434/laplaptech_pipeline_dev
```

Tên `_dev` được giữ vì guard kiểm tra chính xác host/database/user của warehouse local. PostgreSQL cũ ở `localhost:5432`, nếu có, là instance khác. Test Connection tới cổng 5432 không xác nhận warehouse Docker đã được cập nhật.

## 2. Điều kiện và phạm vi hỗ trợ

- Git để clone repo; Docker Desktop đã chạy **Linux containers**, PowerShell và Internet cho lần build/extract đầu.
- `docker version` có phần Server; `docker info --format '{{.OSType}}'` trả `linux`.
- Chuẩn bị login ClickHouse public. `.env.example` có host/user/database nhưng password là placeholder; cần lấy credential truy cập từ người duy trì dataset/repo. Không cần tài khoản Neon để chạy stack.
- Không cần Python/dbt/PostgreSQL trên host cho pipeline/dashboard Docker. Python trên host chỉ cần khi dùng verifier hoặc CLI/test theo các guide riêng.
- Các cổng host mặc định `8080`, `8501`, `55434` phải trống. Xem `docker ps` và ứng dụng đang chạy trước khi chọn cổng khác.
- Tài liệu dùng Windows Docker Desktop. Native Linux/socket GID và fresh-machine installation chưa được suy ra từ report cutover trên workspace có sẵn.

Stack dùng Airflow 3.3.2/Python 3.13/LocalExecutor, Docker provider 4.6.1 và PostgreSQL 16. Runtime có 85 package đã pin. Giới hạn hiện hành theo [resource policy dùng chung](resources.md); bootstrap giữ policy vận hành đã có. Máy ở lần cutover cấp khoảng 8 GB RAM cho Docker; task ingest dùng khoảng 1,7 GB trong giới hạn 2 GB. Đây là số đo của lần chạy đó, không phải bảo đảm cho mọi volume nguồn.

## 3. Cài đặt hoặc cập nhật từ repo

Nếu chưa có repo, tại thư mục cha muốn chứa project trong PowerShell:

```powershell
git clone https://github.com/PhucTruong11/laplaptech_pipeline.git
if ($LASTEXITCODE -ne 0) { throw 'Clone failed; inspect Git output before continuing.' }
Set-Location -LiteralPath laplaptech_pipeline
```

Nếu đã có repo thì dùng thư mục đó. Mở PowerShell tại **root repository** và kiểm tra có `scripts/Start-LocalStack.ps1`; nếu thiếu, cần lấy đúng branch/revision đã phát hành Docker stack. Không chạy bootstrap từ thư mục `infra/airflow-platform`.

Tạo `.env` chỉ khi chưa có:

```powershell
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
# Điền CLICKHOUSE_PASSWORD và kiểm tra login nguồn trong .env bằng editor.
.\scripts\Start-LocalStack.ps1
```

**Checkpoint:** script kết thúc thành công, tạo pool RAM dùng chung `platform_memory` bốn slot và báo địa chỉ Airflow. Dashboard được giữ tắt để tiết kiệm RAM; bật riêng sau khi có Gold. Nếu có lỗi, sửa lỗi đầu tiên trước khi chạy lại; không xóa volumes để thử.

Lần đầu chỉ cần Docker bootstrap → đăng nhập Airflow → unpause/trigger → bốn task success → mở dashboard theo mục 4. Không cần chạy thêm `dbt run` hoặc cài dbt lên host. Dùng [dbt guide](dbt.md) khi muốn học/chạy từng lệnh, [testing](testing.md) nếu muốn kiểm thử bằng fixture trước khi dùng nguồn thật.

Bootstrap mặc định dùng workspace sibling `airflow-platform` cạnh thư mục repo. Nó giữ `.env`/keys Airflow đã có, backup cấu hình trước khi cập nhật, build images và nhập **chỉ** `CLICKHOUSE_*`/`INGESTION_*` vào config volume. Target PostgreSQL của task được đặt cố định về warehouse Docker.

Bootstrap tạm pause mọi DAG đang unpaused trên platform trước khi kiểm tra queued/running runs toàn platform. Nếu vẫn có active run, nó trả trạng thái pause ban đầu rồi dừng; đợi run kết thúc và thử lại. Trong lúc cập nhật, DAG được giữ paused để lịch không tạo writer mới. Thành công sẽ khôi phục trạng thái pause ban đầu. Nếu cập nhật dở dang gặp lỗi, DAG giữ paused: sửa lỗi và chạy lại bootstrap, hoặc kiểm tra cấu hình/runtime rồi chủ động unpause. Platform mới không tự bật DAG.

Có thể chỉ định workspace/cổng:

```powershell
$platformDir = Join-Path (Split-Path -Path (Get-Location).Path -Parent) 'airflow-platform'
.\scripts\Start-LocalStack.ps1 -PlatformDir $platformDir -WarehousePort 55434 -DashboardPort 8501
```

Đổi `WarehousePort` chỉ đổi cổng host; task vẫn dùng port 5432. Nếu chạy Python từ host, cập nhật `POSTGRES_PORT` trong root `.env` tương ứng. Không thay host/port nội bộ của runtime guard.

Source DAG duy nhất là [`infra/laplaptech/dags/laplaptech_pipeline.py`](../../infra/laplaptech/dags/laplaptech_pipeline.py). `LAPLAPTECH_PROJECT_DIR` trong `.env` platform trỏ đến repo và mount DAG read-only. Không copy thêm file Python cùng DAG ID vào platform.

Đọc `AIRFLOW_ADMIN_USERNAME`/`AIRFLOW_ADMIN_PASSWORD` trong **`.env` của platform** để đăng nhập Airflow. `.env.example` chỉ có placeholder. Scheduler được cấp Docker socket để tạo task container trên máy local tin cậy.

## 4. Chạy và kiểm chứng pipeline

1. Mở Airflow, đợi `laplaptech_pipeline` xuất hiện và không có import error.
2. Unpause DAG. Khi bật lần đầu, Airflow có thể tạo lượt mới nhất đã đến hạn; các run vẫn chạy tuần tự.
3. Trigger manual với `full_refresh=true` khi cần rebuild Silver incremental từ raw.
4. Kiểm tra cả bốn task `ingest`, `freshness`, `build`, `reconcile` success; đọc Task Logs khi có task lỗi.
5. Sau khi Gold đã được build, bật dashboard bằng lệnh ở dưới rồi mở cổng 8501. Healthcheck chờ HTTP và đủ 12 Gold relations; DB rỗng có thể làm dashboard chưa healthy đến khi build đầu xong.

```powershell
$platformDir = Join-Path (Split-Path -Path (Get-Location).Path -Parent) 'airflow-platform'
Set-Location -LiteralPath $platformDir
# Trên máy người duy trì, dừng brewlite-db trước nếu còn chạy: cả hai service
# cùng bật sẽ vượt ngân sách RAM khai báo. Người clone không có BrewLite bỏ qua bước này.
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard up -d dashboard
python check_resources.py
```

Khi xem xong, `docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml stop dashboard` trả lại 512 MiB ngân sách cho BrewLite. Nếu cần dùng cả hai liên tục, phải đo tải và phân bổ lại giới hạn trên máy trước; audit sẽ báo lỗi nếu bật cùng lúc với cấu hình hiện tại.

Scheduled run mặc định `full_refresh=false`, lịch **08:15 và 16:45**, timezone `Asia/Ho_Chi_Minh`, `catchup=False`. `max_active_runs=1`, `max_active_tasks=1` giới hạn writer của DAG; pool chung `platform_memory` có bốn slot, mỗi task chiếm slot theo RAM. Không chạy ingestion thủ công song song trên cùng warehouse.

| Task | Công việc | Chính sách |
|---|---|---|
| `ingest` | Snapshot atomic từng bảng cho 5 bảng; event dedup/upsert với lookback | Timeout 45 phút, retry 1 lần sau 2 phút |
| `freshness` | `dbt source freshness` | Warn 24 giờ/error 48 giờ; timeout 5 phút, không retry |
| `build` | 31 model và 47 data tests; optional `--full-refresh` | Timeout 30 phút, retry 1 lần sau 2 phút |
| `reconcile` | Inventory, duplicate ID, raw/Bronze/Silver và daily events/session-days | Timeout 10 phút, không retry |

Task container dùng UID 10001, config read-only, artifact volume riêng; dataset không qua XCom. Asset raw cập nhật khi ingest thành công; validated Gold chỉ phát sau reconcile. Assets lưu metadata dữ liệu sẵn sàng, không chứa dataset và không thay lịch cron.

`full_refresh` chỉ thay lệnh dbt build; nó không lấy lại mọi version nguồn bị sửa/xóa. Gold chưa publish nguyên khối: build fail giữa chừng có thể để lại các mart khác phiên bản. Reconcile/Asset success không phải transaction chung cho toàn warehouse.

Tại **root repo**, verifier chuẩn kiểm tra hệ thống và dữ liệu hiện có, mặc định không trigger ETL:

```powershell
python scripts/verify_local_stack.py --output scratch/system-check.json
```

Khi cần nghiệm thu một lượt pipeline thật mới, chọn thời điểm phù hợp và thêm `--run-pipeline`; lệnh này có đọc nguồn và ghi warehouse:

```powershell
python scripts/verify_local_stack.py --run-pipeline --output scratch/system-check.json
```

Đọc kết quả và thời điểm trong report; không dùng report cutover cũ làm kết quả của lượt kiểm tra vừa chạy.

Nếu đã bật dashboard và muốn kiểm tra thêm giao diện, dùng `python scripts/verify_local_stack.py --with-dashboard --output scratch/system-check-dashboard.json`.

## 5. Vận hành hằng ngày

Các lệnh dưới đây chạy tại **workspace platform**, luôn dùng cả hai Compose file. Từ root repo có thể chuyển sang workspace mặc định bằng:

```powershell
$platformDir = Join-Path (Split-Path -Path (Get-Location).Path -Parent) 'airflow-platform'
Set-Location -LiteralPath $platformDir
```

Nếu đã chọn workspace khác, dùng đúng đường dẫn đó.

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml ps
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml logs --tail 60 airflow-scheduler airflow-dag-processor dashboard
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec airflow-dag-processor airflow dags list-import-errors
```

**Checkpoint:** năm service nền healthy; dashboard tùy chọn. Init `Exited (0)` là bình thường; danh sách import errors rỗng. Health không thay cho kiểm chứng DAG/data tests.

Khi nghỉ, từ root repo dùng công tắc dưới đây. `Off` kiểm tra không còn run/task đang chạy rồi dừng **toàn bộ** Compose `airflow-platform`: Airflow dùng chung, metadata PostgreSQL, warehouse LaplapTech và dashboard nếu đã bật. BrewLite thuộc Compose khác nên không bị ảnh hưởng. `On` bật lại Airflow và warehouse; dashboard vẫn tắt.

```powershell
.\scripts\Set-LocalStack.ps1 -State Status
.\scripts\Set-LocalStack.ps1 -State Off
.\scripts\Set-LocalStack.ps1 -State On
```

Nếu đang ở workspace `D:\Dev\Project\airflow-platform`, các lệnh Compose tương đương là:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard stop -t 60
```

Khi chạy tiếp, mở Docker Desktop và đợi Engine ready, sau đó:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml up -d
```

Không thêm `-v`/`--volumes` khi dừng thường ngày. Dữ liệu, metadata, logs và artifacts nằm trong named volumes. Docker/laptop tắt thì scheduler/dashboard dừng; CI không chạy bù ETL. Lượt kế tiếp dùng watermark + lookback; `catchup=False` không sinh toàn bộ lịch bị lỡ và không thay bounded backfill.

## 6. Sửa code và cấu hình

| Bạn sửa gì? | Thao tác |
|---|---|
| DAG | Lưu source trong repo, chờ processor parse; không copy bản thứ hai |
| Ingestion/dbt/entrypoint | Chạy bootstrap để pause/cập nhật an toàn, hoặc pause DAG và đợi hết active run trước khi rebuild thủ công |
| Dashboard | Rebuild dashboard từ runtime hiện tại, recreate service |
| ClickHouse login | Sửa file nguồn rồi chạy lại bootstrap khi không có run active |
| RAM/CPU, dbt threads, thêm ngân sách project | Drain toàn platform, sửa policy vận hành rồi bootstrap theo [resource guide](resources.md) |
| Airflow encryption/login keys | Giữ cùng metadata; không sinh lại `.env` để xử lý lỗi kết nối |

Cách khuyến nghị sau khi sửa runtime hoặc config là chạy lại bootstrap từ root repo; script giữ các lịch đang bật trên toàn platform paused suốt quá trình rồi khôi phục trạng thái cũ. Nếu rebuild thủ công bằng các lệnh dưới đây, pause DAG và đợi hết queued/running runs trước; chỉ unpause sau khi build/recreate và kiểm tra thành công.

Tại **root repo**:

```powershell
docker build -f infra/laplaptech/runtime.Dockerfile -t laplaptech-runtime:local-v1 .
docker build -f infra/laplaptech/dashboard.Dockerfile -t laplaptech-dashboard:local-v1 .
```

Sau đó tại **workspace platform**:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard up -d dashboard
```

Root `.env` phục vụ Python trên host; `.env` platform phục vụ Airflow; `/run/laplaptech/pipeline.env` trên config volume phục vụ task. Sửa file trên host không tự sửa config volume đã nạp. Biến môi trường shell có thể ưu tiên hơn root `.env` khi chạy CLI trên host.

## 7. Artifacts và backup

Mỗi stage lưu `/artifacts/<sha256(run_id)[:20]>/<stage>/attempt-<try>-<uuid>/report.json`. Build có `target/manifest.json`, `target/run_results.json`; freshness có `target/sources.json`; log dbt ở `logs/`. Nếu container chưa được tạo hoặc dbt chưa chạy, artifact tương ứng có thể chưa tồn tại; đọc Airflow Task Logs để phân biệt.

Liệt kê stage reports từ image runtime:

```powershell
docker run --rm --entrypoint python --mount type=volume,source=laplaptech-lab-artifacts,target=/artifacts,readonly laplaptech-runtime:local-v1 -c 'from pathlib import Path; print("\n".join(str(p) for p in Path("/artifacts").rglob("report.json")))'
```

Để backup, pause DAG và đợi hết queued/running runs. Tại **workspace platform**, tạo một thư mục theo thời điểm và dump hai database. File binary được tạo trong container rồi copy ra host, dùng được cả với Windows PowerShell 5:

```powershell
$compose = @('compose', '--env-file', '.env', '-f', 'compose.yaml', '-f', 'compose.laplaptech.yaml')
$stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
$backupDir = Join-Path (Get-Location).Path ('backups/manual-' + $stamp)
New-Item -ItemType Directory -Path $backupDir -ErrorAction Stop | Out-Null
foreach ($item in @(
    @{ Service = 'warehouse-laplaptech'; User = 'laplaptech'; Database = 'laplaptech_pipeline_dev' },
    @{ Service = 'metadata'; User = 'airflow'; Database = 'airflow_meta' }
)) {
    $dumpFile = '/tmp/' + $item.Database + '-' + $stamp + '.dump'
    docker @compose exec -T $item.Service pg_dump -U $item.User -d $item.Database -Fc -f $dumpFile
    if ($LASTEXITCODE -ne 0) { throw 'pg_dump failed; inspect the database error.' }
    $destination = Join-Path $backupDir ($item.Database + '.dump')
    docker @compose cp ($item.Service + ':' + $dumpFile) $destination
    if ($LASTEXITCODE -ne 0) { throw 'Copying the dump failed; preserve the container dump.' }
}
Copy-Item -LiteralPath '.env' -Destination (Join-Path $backupDir '.env') -ErrorAction Stop
Get-ChildItem -LiteralPath $backupDir -Force
```

**Checkpoint:** có hai file `.dump` và `.env` platform trong cùng thư mục backup. Giữ `.env`/Fernet key cùng metadata backup, không commit. Giữ thêm source `.env` riêng nếu cần tái tạo credential nguồn. Chỉ restore vào database/volume tách biệt rồi đối chiếu counts/run history; archive đọc được chưa xác nhận đã diễn tập restore. Unpause DAG khi hoàn tất backup. Logs/artifacts hiện chưa có tự động retention.

## 8. Xử lý lỗi

| Triệu chứng | Kiểm tra và cách xử lý |
|---|---|
| Không tìm thấy `dockerDesktopLinuxEngine`/Docker daemon | Mở Docker Desktop, đợi Engine ready, kiểm tra `docker version` có Server rồi chạy `up -d` |
| PowerShell báo `running scripts is disabled` | Chạy `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned` trong chính terminal đó rồi chạy lại bootstrap; scope Process chỉ áp dụng cho phiên hiện tại |
| Port already allocated | Xem `docker ps`; chọn cổng host trống qua bootstrap. Giữ nguyên port nội bộ của service |
| PostgreSQL `28P01` | Nhập lại user `laplaptech`, password local; bỏ khoảng trắng đầu/cuối và mật khẩu cũ client đã lưu. Kiểm tra JDBC URL dùng đúng 55434/dev DB |
| Test Connection tới 5432 thành công | Đó có thể là PostgreSQL cũ; dùng đúng 55434 để xem warehouse Docker |
| Airflow login không dùng được | Đọc `.env` platform thật; đổi `.env.example` không đổi user đã có trong metadata |
| DAG không thấy/import error | Kiểm tra `LAPLAPTECH_PROJECT_DIR`, source mount, log DAG processor và `dags list-import-errors` |
| Task queued mãi | Xem scheduler health, trạng thái paused, pool và run trước; đừng chỉ restart API |
| Bootstrap lỗi rồi DAG vẫn paused | Giữ pause để sửa lỗi, chạy lại bootstrap hoặc kiểm tra cấu hình/runtime trước khi unpause; không tạo mới `.env` để thử |
| Bootstrap báo scheduler container không chạy, nhưng metadata còn chạy | Tại workspace platform, dùng đủ hai Compose file chạy `up -d`, đợi scheduler ready rồi chạy bootstrap lại; bootstrap cần scheduler hiện có để pause DAG trước cập nhật |
| Freshness error | Đối chiếu `sources.json` và timestamp nguồn; không bỏ kiểm tra chỉ để run xanh |
| Task exit 137 | Xem RAM và task log, đo workload; giảm đồng thời trước khi tăng giới hạn |
| Dashboard unhealthy | Đợi build đầu xong, xem log dashboard và kết nối DB; healthcheck yêu cầu đủ Gold relations |

## 9. Phạm vi cloud và bằng chứng

Workflow ETL `.github/workflows/sync-pipeline.yml` được gỡ khỏi default branch tại commit `7e449edb7b6a1f10eab1d4fde2285a3c32ba3b6c`; report cutover ghi việc kiểm tra remote tại thời điểm đó. `validate.yml` tiếp tục kiểm thử code/fixture/dbt/AppTest, không dùng Neon secrets.

Cutover không xóa project Neon. Nếu đã tự xóa qua Neon Console sau đó, trạng thái cloud không được suy ra từ report local. Warehouse hiện hành không phụ thuộc Neon. Xem [kiến trúc và giới hạn](../architecture/data_flow.md) cho restore drill, bounded backfill, quarantine và publish snapshot.

Các thao tác chuyên sâu có nguồn hướng dẫn riêng: [Airflow và DAG](airflow.md), [dbt CLI và model](dbt.md), [ingestion/backfill](ingestion.md), [kiểm thử fixture và hệ thống](testing.md).

Tham khảo: [DockerOperator](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html), [MultipleCronTriggerTimetable](https://airflow.apache.org/docs/apache-airflow/3.3.2/_api/airflow/timetables/trigger/index.html), [Airflow Assets](https://airflow.apache.org/docs/apache-airflow/3.3.2/authoring-and-scheduling/asset-scheduling.html), [Streamlit Docker](https://docs.streamlit.io/deploy/tutorials/docker).
