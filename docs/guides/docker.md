# Docker: cài đặt và vận hành LapLapTech

**Đọc nhanh:** lần đầu làm mục 2 → 3 → 4. Những lần sau dùng mục 5 để bật/tắt. Chỉ xem mục 6–9 khi sửa cấu hình, sao lưu hoặc xử lý lỗi. [Data flow](../architecture/data_flow.md) giải thích hệ thống.

## 1. Mục đích và cấu hình

Airflow đọc ClickHouse theo batch, nạp warehouse PostgreSQL rồi chạy freshness, graph model Cosmos, data tests và reconcile. Streamlit đọc Gold. GitHub Actions chạy CI với fixture. [Hướng dẫn kiểm thử](testing.md).

### Lệnh nhanh để lưu vào Notepad

Chạy trong PowerShell. Từ **root repo**, xác định workspace mặc định một lần; nếu bootstrap ở vị trí khác, đặt `$platformDir` theo vị trí bạn đã chọn:

```powershell
$platformDir = Join-Path (Split-Path -Path (Get-Location).Path -Parent) 'airflow-platform'
```

Trước khi dừng **toàn stack**, đợi mọi DAG run/task kết thúc hoặc dùng công tắc `.\scripts\Set-LocalStack.ps1 -State Off` ở mục 5 để kiểm tra. Có thể dừng riêng dashboard trong lúc pipeline chạy. `stop` giữ database trong named volume.

```text
LapLapTech
- Check toàn bộ stack, kể cả container đang tắt
Set-Location $platformDir
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard ps -a

- Off toàn bộ stack Docker Airflow/LapLapTech
Set-Location $platformDir
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard stop -t 60

- On Airflow và warehouse LapLapTech
Set-Location $platformDir
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml up -d

- On dashboard sau khi đã On stack Airflow/LapLapTech
Set-Location $platformDir
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard up -d dashboard

- Off riêng dashboard
Set-Location $platformDir
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard stop dashboard
```

Airflow là hạ tầng dùng chung: `Off toàn bộ stack` cũng dừng lịch của project khác nếu sau này chúng được nối vào platform. Kiểm tra các DAG đang chạy trước khi tắt. Khi bật thêm service trên máy, chạy `python check_resources.py` tại workspace platform để kiểm tra ngân sách RAM.

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
| User để xem Gold | `laplaptech_dashboard` |
| Password | Giá trị `LAPLAPTECH_DASHBOARD_PASSWORD` trong `.env` platform, đọc bằng editor local |
| SSL mode | `prefer` |

```text
jdbc:postgresql://127.0.0.1:55434/laplaptech_pipeline_dev
```

Tên `_dev` được giữ vì guard kiểm tra chính xác host/database/user của warehouse local. PostgreSQL cũ ở `localhost:5432`, nếu có, là instance khác. Test Connection tới cổng 5432 không xác nhận warehouse Docker đã được cập nhật.

`laplaptech_pipeline` chạy ingestion/dbt, sở hữu đối tượng trong bốn schema của dự án, không phải superuser. Mật khẩu là `LAPLAPTECH_PIPELINE_PASSWORD`. Dashboard chỉ có SELECT trên Gold, không đọc raw và không ghi dữ liệu. `laplaptech` cùng `WAREHOUSE_PASSWORD` dành riêng cho quản trị/backup; không đưa credential này vào app hay task. Bootstrap giữ mật khẩu đã có khi chạy lại.

## 2. Điều kiện và phạm vi hỗ trợ

- Git để clone repo, PowerShell 7 và Docker Desktop ở chế độ **Linux containers**. Lần build và nạp nguồn đầu tiên cần kết nối Internet.
- `docker version` có phần Server; `docker info --format '{{.OSType}}'` trả `linux`.
- Chuẩn bị login ClickHouse public. `.env.example` có host/user/database nhưng password là placeholder; cần lấy credential truy cập từ người duy trì dataset/repo. Không cần tài khoản Neon để chạy stack.
- Không cần Python/dbt/PostgreSQL trên host cho pipeline/dashboard Docker. Python trên host chỉ cần khi dùng verifier hoặc CLI/test theo các guide riêng.
- Các cổng host mặc định `8080`, `8501`, `55434` phải trống. Xem `docker ps` và ứng dụng đang chạy trước khi chọn cổng khác.
- Tài liệu dùng Windows Docker Desktop. Native Linux/socket GID và fresh-machine installation chưa được suy ra từ report cutover trên workspace có sẵn.

Stack dùng Airflow 3.3.2/Python 3.13/LocalExecutor, Docker provider 4.6.1, Cosmos 1.15.1 và PostgreSQL 16. Runtime có 85 package đã pin. Bootstrap giữ policy vận hành đã có. [Ngân sách RAM/CPU và cách kiểm tra](resources.md).

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
.\scripts\Start-LocalStack.ps1 -InitializePlatform
```

**Checkpoint:** script kết thúc thành công và tạo pool RAM dùng chung `platform_memory` bốn slot. Mở Airflow ở [localhost:8080](http://localhost:8080) theo mặc định. Lần cài mới bật chế độ Cosmos và dashboard được bật riêng sau khi có Gold.

Lần đầu dùng Docker bootstrap → đăng nhập Airflow → unpause/trigger → đợi reconcile thành công → mở dashboard theo mục 4. Bootstrap mới build Cosmos và xuất manifest cùng runtime. Dùng [dbt guide](dbt.md) khi muốn học từng lệnh và [testing](testing.md) khi muốn kiểm bằng fixture.

`-InitializePlatform` chỉ chấp nhận thư mục mới/rỗng, dành cho người clone dự án lần đầu. Bootstrap mặc định dùng workspace sibling `airflow-platform`. Nếu đã có platform, chạy `.\scripts\Start-LocalStack.ps1` **không có cờ khởi tạo**: script kiểm tra integration, build hai image của LapLapTech, cấp quyền/nạp config riêng và giữ nguyên Compose, `.gitignore`, operator, registry, keys và hướng dẫn platform. Không tự cập nhật hạ tầng chung từ template trong repo.

Source config chỉ nhập `CLICKHOUSE_*`/`INGESTION_*`. Hai project passwords được sinh một lần trong `.env` platform; config volume chỉ chứa password pipeline, dashboard chỉ nhận password reader. Role và ownership được cập nhật trong một transaction, giới hạn đúng database LapLapTech; không dùng `REASSIGN OWNED` toàn cluster. Schema/tables Gold mới do pipeline tạo tiếp tục cấp SELECT cho dashboard bằng default privileges.

Bootstrap tạm pause mọi DAG đang unpaused trên platform trước khi kiểm tra queued/running runs toàn platform. Nếu vẫn có active run, nó trả trạng thái pause ban đầu rồi dừng; đợi run kết thúc và thử lại. Trong lúc cập nhật, DAG được giữ paused để lịch không tạo writer mới. Thành công sẽ khôi phục trạng thái pause ban đầu. Nếu cập nhật dở dang gặp lỗi, DAG giữ paused: sửa lỗi và chạy lại bootstrap, hoặc kiểm tra cấu hình/runtime rồi chủ động unpause. Platform mới không tự bật DAG.

Có thể chỉ định workspace mới và cổng khi cài lần đầu:

```powershell
$platformDir = Join-Path (Split-Path -Path (Get-Location).Path -Parent) 'airflow-platform'
.\scripts\Start-LocalStack.ps1 -InitializePlatform -PlatformDir $platformDir -WarehousePort 55434 -DashboardPort 8501
```

Cổng tùy chọn chỉ dùng khi khởi tạo mới bằng `-InitializePlatform`. Với platform đang có, script kiểm tra và giữ nguyên cổng/path đã đăng ký; thay đổi chúng trực tiếp tại platform sau khi review các project dùng chung. Nếu chạy Python từ host, dùng user `laplaptech_pipeline`, password riêng và cập nhật `POSTGRES_PORT` trong root `.env`; không thay host/port nội bộ của runtime guard.

Source DAG duy nhất là [`infra/laplaptech/dags/laplaptech_pipeline.py`](../../infra/laplaptech/dags/laplaptech_pipeline.py). `LAPLAPTECH_PROJECT_DIR` trong `.env` platform trỏ đến repo và mount DAG read-only. Không copy thêm file Python cùng DAG ID vào platform.

Đọc `AIRFLOW_ADMIN_USERNAME`/`AIRFLOW_ADMIN_PASSWORD` trong **`.env` của platform** để đăng nhập Airflow. `.env.example` chỉ có placeholder. Scheduler được cấp Docker socket để tạo task container trên máy local tin cậy.

## 4. Chạy và kiểm chứng pipeline

1. Mở Airflow, đợi `laplaptech_pipeline` xuất hiện và không có import error.
2. Unpause DAG và xem tab **Runs**. Khi khởi động lại, Airflow có thể tiếp tục lượt đã đến hạn còn chờ. Đợi lượt đang chạy nếu mục đích là kiểm tra pipeline.
3. Trigger manual khi cần một lượt riêng. Dùng mặc định `full_refresh=false` hoặc chọn `true` để rebuild Silver incremental từ Raw.
4. Kiểm tra `ingest`, `freshness`, nhóm `transform`, `dbt_tests`, collector và `reconcile` success. Đọc Task Logs của model lỗi. [Hướng dẫn Cosmos](../plans/cosmos.md) ghi cách phát hành và rollback.
5. Sau khi Gold đã được build, bật dashboard theo [mục 5](#bật-tắt-riêng-dashboard). Healthcheck kiểm tra HTTP và đủ 12 Gold relations trong warehouse.

Scheduled run mặc định `full_refresh=false`, lịch **07:00 và 19:00**, timezone `Asia/Ho_Chi_Minh`, `catchup=False`. `max_active_runs=1`, `max_active_tasks=1` giới hạn writer của DAG; pool chung `platform_memory` có bốn slot, mỗi task chiếm slot theo RAM. Không chạy ingestion thủ công song song trên cùng warehouse.

| Task | Công việc | Chính sách |
|---|---|---|
| `ingest` | Snapshot atomic từng bảng cho 5 bảng; event dedup/upsert với lookback | Timeout 45 phút, retry 1 lần sau 2 phút |
| `freshness` | `dbt source freshness` | Warn 24 giờ/error 48 giờ; timeout 5 phút, không retry |
| `transform` | 31 model trong task riêng; optional `--full-refresh` | Mỗi model timeout 30 phút, retry 1 lần sau 2 phút |
| `dbt_tests` và `collect_dbt_results` | 47 data test và kiểm artifact của release/run | Mỗi task timeout 30 phút, không retry tự động |
| `reconcile` | Inventory, duplicate ID, raw/Bronze/Silver và daily events/session-days | Timeout 10 phút, không retry |

Task container dùng UID 10001, config read-only, artifact volume riêng; dataset không qua XCom. Asset raw cập nhật khi ingest thành công; validated Gold chỉ phát sau reconcile. Assets lưu metadata dữ liệu sẵn sàng, không chứa dataset và không thay lịch cron.

`full_refresh=true` yêu cầu các task model Cosmos chạy `dbt run --full-refresh`. Chế độ legacy dùng `dbt build --full-refresh`. Ingestion vẫn giữ quy tắc snapshot và lookback. Gold được cập nhật theo từng model, nên build lỗi giữa chừng có thể để lại các mart ở những phiên bản khác nhau.

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
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard logs --tail 60 airflow-scheduler airflow-dag-processor dashboard
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec airflow-dag-processor airflow dags list-import-errors
```

**Checkpoint:** năm service nền healthy; dashboard tùy chọn. Init `Exited (0)` là bình thường; danh sách import errors rỗng. Health không thay cho kiểm chứng DAG/data tests.

Khi nghỉ, từ root repo dùng công tắc dưới đây. `Off` kiểm tra không còn run/task đang chạy rồi dừng Airflow dùng chung, metadata PostgreSQL, warehouse LapLapTech và dashboard nếu đang bật. `On` khởi động Airflow và warehouse. Muốn dùng dashboard sau khi bật lại, chạy lệnh ở mục dưới. Các lệnh tác động trong phạm vi Compose project `airflow-platform`.

```powershell
.\scripts\Set-LocalStack.ps1 -State Status
.\scripts\Set-LocalStack.ps1 -State Off
.\scripts\Set-LocalStack.ps1 -State On
```

Nếu đang ở workspace platform đã chọn, các lệnh Compose tương đương là:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard stop -t 60
```

Khi chạy tiếp, mở Docker Desktop và đợi Engine ready, sau đó:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml up -d
```

Dữ liệu, trạng thái pause của DAG, logs và artifacts được giữ trong named volumes khi dùng `stop`. Docker hoặc laptop tắt thì scheduler dừng. Khi stack khởi động và DAG đang unpaused, lượt còn chờ có thể chạy, dùng watermark và lookback hiện có. Với `catchup=False`, Airflow không chạy lại toàn bộ lịch bị lỡ. [Recovery theo khoảng thời gian](ingestion.md#4-backfill-event-theo-khoảng-thời-gian).

### Bật tắt riêng dashboard

Chạy ở workspace platform. Airflow và warehouse tiếp tục chạy khi dashboard tắt, nên có thể xem kết quả lượt **07:00 / 19:00** trên Airflow trước khi mở dashboard lại.

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard up -d dashboard
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard stop dashboard
```

Mở [localhost:8501](http://localhost:8501) theo mặc định. Nếu cổng host đang bận, đặt `$env:DASHBOARD_PORT = '8502'` trong PowerShell trước lệnh `up` và mở [localhost:8502](http://localhost:8502). Biến này chỉ tồn tại trong phiên PowerShell hiện tại. Dùng `docker compose` với hai file trên và `--profile dashboard ps dashboard` để xem cổng thực tế. Dashboard có ngân sách tối đa 512 MiB khi chạy. Kiểm ngân sách từ workspace platform bằng `python check_resources.py`.

## 6. Sửa code và cấu hình

| Bạn sửa gì? | Thao tác |
|---|---|
| DAG | Lưu source trong repo, chờ processor parse; không copy bản thứ hai |
| Ingestion/dbt/entrypoint | Chạy bootstrap để build runtime, phát hành manifest và chờ DAG nhận release mới ở chế độ Cosmos |
| Dashboard | Rebuild dashboard từ runtime hiện tại, recreate service |
| ClickHouse login | Sửa file nguồn rồi chạy lại bootstrap khi không có run active |
| RAM/CPU, dbt threads, thêm ngân sách project | Drain toàn platform, áp dụng tại workspace platform theo [resource guide](resources.md); bootstrap chỉ đọc policy đã cấu hình |
| Airflow encryption/login keys | Giữ cùng metadata; không sinh lại `.env` để xử lý lỗi kết nối |

Sau khi sửa runtime hoặc config, chạy `.\scripts\Start-LocalStack.ps1` từ root repo. Script giữ các lịch bị ảnh hưởng ở trạng thái paused trong lúc cập nhật rồi khôi phục trạng thái cũ khi hoàn tất. DAG Cosmos gọi tag theo SHA của manifest, nên build lại `laplaptech-runtime:local-v1` một mình chưa cập nhật DAG. [Phát hành runtime Cosmos thủ công](../plans/cosmos.md#1-chuẩn-bị-và-phát-hành-runtime).

Nếu chỉ sửa Streamlit, rebuild dashboard tại **root repo**:

```powershell
docker build -f infra/laplaptech/dashboard.Dockerfile -t laplaptech-dashboard:local-v1 .
if ($LASTEXITCODE -ne 0) { throw 'Dashboard build failed.' }
```

Sau đó tại **workspace platform**:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml --profile dashboard up -d --no-build --no-deps dashboard
```

Root `.env` phục vụ Python trên host; `.env` platform phục vụ Airflow; `/run/laplaptech/pipeline.env` trên config volume phục vụ task. Sửa file trên host không tự sửa config volume đã nạp. Biến môi trường shell có thể ưu tiên hơn root `.env` khi chạy CLI trên host.

## 7. Artifacts và backup

Artifact nằm trong volume `laplaptech-lab-artifacts`. Lấy đường dẫn của đúng run/attempt từ log task. Model/test Cosmos có `target/run_results.json`, collector ghi `cosmos-summary.json` và freshness ghi `target/sources.json`. [Bảng đường dẫn artifacts theo task](airflow.md#5-assets-logs-và-artifacts).

Liệt kê task reports và Cosmos summaries từ image runtime:

```powershell
docker run --rm --network none --entrypoint python --mount type=volume,source=laplaptech-lab-artifacts,target=/artifacts,readonly laplaptech-runtime:local-v1 -c 'from pathlib import Path; root = Path("/artifacts"); print("\n".join(str(p) for name in ("report.json", "cosmos-summary.json") for p in sorted(root.rglob(name))))'
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
| Port already allocated | Xem `docker ps` và tiến trình đang nghe trên host. Với dashboard, dùng `DASHBOARD_PORT` theo mục 5. Cổng warehouse trên platform đã có được chỉnh ở workspace platform. |
| PostgreSQL `28P01` | Kiểm đúng role pipeline/dashboard và key tương ứng trong `.env` platform; bỏ mật khẩu cũ client đã lưu. Kiểm JDBC URL dùng đúng 55434/dev DB; không reset mật khẩu admin để chữa kết nối app |
| Test Connection tới 5432 thành công | Đó có thể là PostgreSQL cũ; dùng đúng 55434 để xem warehouse Docker |
| Airflow login không dùng được | Đọc `.env` platform thật; đổi `.env.example` không đổi user đã có trong metadata |
| DAG không thấy/import error | Kiểm tra `LAPLAPTECH_PROJECT_DIR`, source mount, log DAG processor và `dags list-import-errors` |
| Task queued mãi | Xem scheduler health, trạng thái paused, pool và run trước; đừng chỉ restart API |
| Bootstrap lỗi rồi DAG vẫn paused | Giữ pause để sửa lỗi, chạy lại bootstrap hoặc kiểm tra cấu hình/runtime trước khi unpause; không tạo mới `.env` để thử |
| Freshness error | Đối chiếu `sources.json` và timestamp nguồn; không bỏ kiểm tra chỉ để run xanh |
| Task exit 137 | Xem RAM và task log, đo workload; giảm đồng thời trước khi tăng giới hạn |
| Dashboard unhealthy | Đợi build đầu xong, xem log dashboard và kết nối DB; healthcheck yêu cầu đủ Gold relations |
| Dashboard **Today** hiện 0 | Kiểm tra `reconcile` và **Latest received event**. **Refresh data** chỉ đọc lại Gold. Nếu ClickHouse đã có event hôm nay mà Gold chưa có, kiểm tra event có xuất hiện sau lượt ingest gần nhất hay chưa. [Cách dashboard tính ngày](../architecture/data_flow.md#36-dashboard-exposure-và-giới-hạn-phục-vụ-dữ-liệu). |

## 9. Phạm vi cloud và bằng chứng

Workflow ETL `.github/workflows/sync-pipeline.yml` được gỡ khỏi default branch tại commit `7e449edb7b6a1f10eab1d4fde2285a3c32ba3b6c`; report cutover ghi việc kiểm tra remote tại thời điểm đó. `validate.yml` tiếp tục kiểm thử code/fixture/dbt/AppTest, không dùng Neon secrets.

Cutover không xóa project Neon. Nếu đã tự xóa qua Neon Console sau đó, trạng thái cloud không được suy ra từ report local. Warehouse hiện hành không phụ thuộc Neon. Xem [kiến trúc và giới hạn](../architecture/data_flow.md) cho restore drill, bounded backfill, quarantine và publish snapshot.

Các thao tác chuyên sâu có nguồn hướng dẫn riêng: [Airflow và DAG](airflow.md), [dbt CLI và model](dbt.md), [ingestion/backfill](ingestion.md), [kiểm thử fixture và hệ thống](testing.md).

Tham khảo: [DockerOperator](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html), [MultipleCronTriggerTimetable](https://airflow.apache.org/docs/apache-airflow/3.3.2/_api/airflow/timetables/trigger/index.html), [Airflow Assets](https://airflow.apache.org/docs/apache-airflow/3.3.2/authoring-and-scheduling/asset-scheduling.html), [Streamlit Docker](https://docs.streamlit.io/deploy/tutorials/docker).
