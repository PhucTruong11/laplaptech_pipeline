# Chạy và phát triển dbt

## 1. dbt làm gì trong project?

dbt đọc SQL/Jinja trong repo, sắp dependency bằng `source()`/`ref()`, gửi SQL đến PostgreSQL và chạy data tests. Nó không kéo dữ liệu ClickHouse; ingestion phải nạp `raw` trước. Lịch chạy và retry do [Airflow](airflow.md) quản lý.

| Tầng | Schema | Materialization hiện tại |
|---|---|---|
| Nguồn | `raw` | 6 bảng do ingestion quản lý |
| Bronze | `public_bronze` | 6 views ánh xạ nguồn |
| Silver | `public_silver` | 12 views + bảng incremental `silver_user_event_tracking` |
| Gold | `public_gold` | 12 tables phục vụ dashboard |

Source hiện có **31 model, 47 data tests, 6 sources và 1 exposure**. Repo pin **dbt Core 1.12.3**; freshness dùng lệnh `dbt source freshness`. Freshness chỉ cấu hình cho nguồn event: warn sau 24 giờ, error sau 48 giờ.

| Lệnh / khai báo | Vai trò | Khi dùng trong project |
|---|---|---|
| `dbt run` | Tạo/cập nhật các model đã chọn; không chạy data tests | Thử SQL hoặc chạy lại một model khi phát triển |
| `dbt test` | Kiểm tra dữ liệu/model hiện có; không rebuild các model đó | Chạy lại invariant sau khi sửa lỗi hoặc kiểm tra bảng đã build |
| `dbt build` | Build model và chạy tests theo dependency; test error có thể chặn downstream | Stage `build` của Airflow, gồm 31 model và 47 data tests khi chọn toàn project |
| `dbt source freshness` | Đối chiếu timestamp nguồn với ngưỡng warn/error | Stage `freshness`, trước build; không nằm trong `dbt build` |
| `exposures:` trong YAML | Khai báo ứng dụng/dashboard nào sử dụng các model | Ghi lineage của Streamlit và chọn upstream cần run/test/build |
| `dbt docs generate` / `dbt docs serve` | Sinh/mở tài liệu catalog và lineage, gồm exposure | Học model và xem dashboard phụ thuộc bảng nào |

`build` còn hỗ trợ seeds/snapshots nếu project có các resource đó; repo hiện không có seed/snapshot. Luồng mặc định là **ingestion → source freshness → dbt build → reconcile**. Không cần thêm `run` rồi `test` vào Airflow sau một `build` toàn project đã pass. [Command reference](https://docs.getdbt.com/reference/dbt-commands), [dbt build](https://docs.getdbt.com/reference/commands/build), [source freshness](https://docs.getdbt.com/reference/commands/source).

Source cấu hình chuẩn: [`dbt_project.yml`](../../dbt/dbt_project.yml), [`profiles.yml`](../../dbt/profiles.yml), [`sources.yml`](../../dbt/models/sources.yml), [`model_tests.yml`](../../dbt/models/model_tests.yml) và [`dbt/tests/`](../../dbt/tests). Repo chưa dùng package dbt bên ngoài nên không cần bước `dbt deps`.

## 2. Chạy thường ngày qua Airflow

Sau khi [Docker stack](docker.md) đã được dựng:

1. Mở [Airflow](http://localhost:8080), chọn `laplaptech_pipeline`.
2. Trigger với `full_refresh=false` cho lượt thường; dùng `true` khi cần rebuild Silver incremental từ raw.
3. Theo dõi `ingest → freshness → build → reconcile`; cả bốn task cần success.
4. Mở log task để tìm đường dẫn artifacts, rồi xem dashboard.

Airflow gọi [`run_airflow_stage.py`](../../scripts/run_airflow_stage.py) trong image runtime. Wrapper nạp config volume read-only, xác nhận đúng database/user và giữ output riêng theo run/task/attempt. Dataset không đi qua XCom.

Sửa SQL/model trong repo **chưa thay code trong image task**. Chạy lại bootstrap theo [Docker guide](docker.md) để rebuild/cập nhật an toàn, sau đó trigger run mới. Sửa DAG Python thì chờ DAG processor parse, không cần rebuild runtime.

## 3. Chạy từng stage bằng Docker

Chỉ dùng khi cần điều tra thủ công. Trước hết pause các DAG đang bật trên platform và đợi hết queued/running runs/task containers. Ghi lại trạng thái để bật lại sau khi hoàn tất. Các lệnh dưới đây không đi qua pool Airflow; chạy tuần tự, trong lúc toàn platform đã drain, để không chiếm thêm ngân sách task. Mẫu stage thủ công dùng tối đa 1 GiB/1 CPU; điều chỉnh theo [policy vận hành](resources.md) khi đổi ngân sách.

Từ **root repo**, sau khi stack/config/artifact volumes đã sẵn sàng:

```powershell
$runId = 'manual-dbt-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
$runtime = @(
    'run', '--rm', '--network', 'laplaptech-lab',
    '--memory', '1024m', '--memory-swap', '1024m', '--cpus', '1', '--pids-limit', '256',
    '-e', 'DBT_THREADS=2', '-e', 'OMP_NUM_THREADS=1', '-e', 'OPENBLAS_NUM_THREADS=1', '-e', 'MKL_NUM_THREADS=1',
    '--mount', 'type=volume,source=laplaptech-lab-config,target=/run/laplaptech,readonly',
    '--mount', 'type=volume,source=laplaptech-lab-artifacts,target=/artifacts',
    '-e', "PIPELINE_RUN_ID=$runId",
    'laplaptech-runtime:local-v1'
)
function Invoke-DbtStage {
    docker @runtime @args
    if ($LASTEXITCODE -ne 0) { throw 'Runtime stage failed; inspect the output before continuing.' }
}
Invoke-DbtStage preflight
Invoke-DbtStage freshness
```

**Checkpoint:** preflight xác nhận `warehouse-laplaptech / laplaptech_pipeline_dev`; freshness không có error. Chỉ tiếp tục build nếu các bước trên đạt:

```powershell
Invoke-DbtStage build
Invoke-DbtStage reconcile
```

Nếu cần rebuild toàn bộ incremental model, dùng `Invoke-DbtStage build --full-refresh` thay cho lệnh build thường. Helper dừng khi stage có exit code khác 0; không chạy tiếp để che lỗi trước. Kiểm tra report/log và đối soát trước khi unpause DAG.

`--full-refresh` của dbt chỉ xử lý raw hiện có; nó không extract lại mọi event ClickHouse hoặc tự sửa dữ liệu nằm ngoài raw. Backfill xem [ingestion](ingestion.md).

## 4. Dùng dbt CLI trên máy để học và sửa model

Cách này phục vụ parse, chọn model, debug kết nối hoặc tạo tài liệu. Chạy từ **root repo** trong PowerShell. Cần Python 3.13; bước Docker setup và root `.env` đã hoàn tất.

```powershell
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
    py -3.13 -m venv .venv
}
$python = '.\.venv\Scripts\python.exe'
& $python -m pip install -r requirements.txt
& $python -m pip check
```

dbt không tự đọc root `.env`; project `profiles.yml` dùng `env_var`. Các ví dụ dùng python-dotenv `--override` để nạp rõ file cấu hình đã chọn. Kiểm tra root `.env` có target host `localhost:55434`, user `laplaptech`, DB `laplaptech_pipeline_dev`, SSL `prefer`. Bên trong task Docker, địa chỉ tương ứng là `warehouse-laplaptech:5432`.

Tạo vùng artifacts riêng cho phiên học này:

```powershell
$dbt = '.\.venv\Scripts\dbt.exe'
$output = Join-Path (Get-Location).Path ('scratch/dbt-manual-' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ'))
$dbtArgs = @(
    '--project-dir', 'dbt', '--profiles-dir', 'dbt',
    '--target-path', (Join-Path $output 'target'),
    '--log-path', (Join-Path $output 'logs')
)
& $python -m dotenv -f .env run --override -- $dbt parse @dbtArgs
& $python -m dotenv -f .env run --override -- $dbt list --resource-type model --output name @dbtArgs
& $python -m dotenv -f .env run --override -- $dbt debug --project-dir dbt --profiles-dir dbt --log-path (Join-Path $output 'logs')
```

**Checkpoint:** parse/list nhận đúng 31 model; debug kiểm tra profile và kết nối. Parse thành công chưa chứng minh SQL/model đã chạy trên dữ liệu.

Khi muốn build từ host, pause DAG và đợi hết active runs như mục 3. Chạy từng lệnh và chỉ tiếp tục khi lệnh trước thành công:

```powershell
& $python -m dotenv -f .env run --override -- $dbt source freshness @dbtArgs
if ($LASTEXITCODE -ne 0) { throw 'Freshness failed; inspect sources.json before build.' }
& $python -m dotenv -f .env run --override -- $dbt build @dbtArgs
```

Đây là writer thủ công vào warehouse đang dùng. Thử fixture hoặc thay đổi có thể phá dữ liệu phải chạy trên database test riêng theo [testing](testing.md), không đổi target fixture thành warehouse active.

## 5. Chọn model, rebuild và chạy test riêng

Các ví dụ dưới đây dùng biến CLI của mục 4, với cùng yêu cầu pause/writer isolation khi build:

```powershell
# Khi phát triển: chạy một model, rồi kiểm tra dữ liệu của model đó.
& $python -m dotenv -f .env run --override -- $dbt run --select gold_brand_interest @dbtArgs
if ($LASTEXITCODE -ne 0) { throw 'Model run failed; inspect dbt logs before testing.' }
& $python -m dotenv -f .env run --override -- $dbt test --select gold_brand_interest @dbtArgs
```

`run` chỉ chạy model đã chọn; upstream phải tồn tại. Dùng `+gold_brand_interest` khi cần chọn thêm ancestors. Hai lệnh trên giúp học riêng từng bước; khi nghiệm thu dùng `build` để model/tests tuân cùng dependency graph:

```powershell
# Xem tập model trước khi chạy: dấu + phía sau lấy downstream.
& $python -m dotenv -f .env run --override -- $dbt list --select 'silver_user_event_tracking+' --resource-type model @dbtArgs

# Rebuild Silver event và các nhánh downstream từ raw hiện có.
& $python -m dotenv -f .env run --override -- $dbt build --full-refresh --select 'silver_user_event_tracking+' @dbtArgs

# Sau khi sửa một invariant, chạy riêng test để điều tra.
& $python -m dotenv -f .env run --override -- $dbt test --select assert_daily_event_counts @dbtArgs
```

`model+` chọn model và descendants; `+model` chọn model và ancestors. Build chọn một nhánh không phải nghiệm thu toàn pipeline. Không cần chạy lại toàn bộ `dbt test` ngay sau một full `dbt build` đã pass, vì build đã chạy data tests. [Graph selection](https://docs.getdbt.com/reference/node-selection/graph-operators).

Ngày/tháng nghiệp vụ dùng `Asia/Ho_Chi_Minh` qua [`business_time.sql`](../../dbt/macros/business_time.sql). Daily KPI và funnel dùng session-day; cộng daily sessions qua nhiều ngày là **session-days**, không phải distinct sessions toàn kỳ. Grain và giới hạn metric xem [data flow](../architecture/data_flow.md).

## 6. Tạo tài liệu dbt và đọc artifacts

Từ phiên CLI mục 4, với database đã có raw/models:

```powershell
& $python -m dotenv -f .env run --override -- $dbt docs generate @dbtArgs
& $python -m dotenv -f .env run --override -- $dbt docs serve --host 127.0.0.1 --port 8081 --no-browser @dbtArgs
```

Mở [localhost:8081](http://localhost:8081) để xem lineage/catalog; `Ctrl+C` dừng docs server. Generate tạo tài liệu, không thay lượt build/test. [dbt docs](https://docs.getdbt.com/reference/commands/cmd-docs).

Ảnh đã lưu để tham khảo: [lineage tổng hợp có exposure](../../assets/lineage/laplaptech-dbt-lineage.png) và [DAG xuất từ giao diện dbt](../../assets/lineage/dbt-dag.png). Đây là snapshot; chạy lại `docs generate` để xem graph theo code hiện tại.

| Artifact | Đọc để biết gì? |
|---|---|
| `manifest.json` | Model/source/test/exposure và dependency đã parse |
| `run_results.json` | Kết quả/timing từng resource của build hoặc test |
| `sources.json` | Kết quả freshness và timestamp nguồn |
| `catalog.json`, `index.html` | Catalog/tài liệu do docs generate sinh |
| `logs/dbt.log` | Chi tiết lỗi và SQL thực thi |

Phiên CLI lưu dưới `$output`; task Airflow lưu trên `laplaptech-lab-artifacts` theo đường dẫn trong task log. Không commit target/logs hoặc chép credential vào report.

## 7. Exposures: khai báo phần sử dụng dữ liệu

Exposure mô tả sản phẩm ở cuối lineage. Project có **`laplaptech_analytics_dashboard`**, khai báo trong [`dbt/models/exposures.yml`](../../dbt/models/exposures.yml): type `dashboard`, owner, description, URL tham chiếu và `depends_on` tới đủ **12 Gold model** mà Streamlit đọc.

Đọc YAML theo thứ tự: `name` để định danh → `type` để biết ứng dụng gì → `depends_on` để biết dữ liệu cần có → `owner` để biết ai phụ trách. URL hiện trỏ repository; dashboard chạy ở địa chỉ local trong Docker guide. `maturity: high` là metadata do tác giả khai báo, không phải kết quả tự động kiểm tra chất lượng.

Sau khi có các biến CLI của mục 4, liệt kê exposure và model upstream mà không chạy SQL model:

```powershell
& $python -m dotenv -f .env run --override -- $dbt list --resource-type exposure --output name @dbtArgs
& $python -m dotenv -f .env run --override -- $dbt list --select '+exposure:laplaptech_analytics_dashboard' --resource-type model --output name @dbtArgs
```

**Checkpoint:** lệnh đầu thấy `laplaptech_analytics_dashboard`; lệnh sau hiện chọn **29 model upstream, gồm đủ 12 Gold**. Dấu `+` phía trước lấy upstream. Hai Silver model chưa có Gold downstream không tự được chọn chỉ vì cùng project. Đây là kết quả kiểm tra selector, chưa chạy SQL/build hay xác nhận các bảng đã tồn tại.

Khi chủ động build nhánh phục vụ dashboard, vẫn pause DAG/đợi hết active runs trước:

```powershell
& $python -m dotenv -f .env run --override -- $dbt build --select '+exposure:laplaptech_analytics_dashboard' @dbtArgs
```

Lệnh trên build/test **các dependency được chọn**. Exposure không tạo table/view, không mở Streamlit, không lập lịch và không tự kiểm tra dashboard có hoạt động hay không; không có lệnh `dbt exposure` để chạy dashboard. Mở docs ở mục 6 để xem exposure trong lineage; chạy AppTest/verifier theo [testing guide](testing.md) để kiểm tra ứng dụng thực tế. Khi app dùng thêm/bớt Gold model, cập nhật cả `depends_on` để tài liệu khớp query thật. [Hướng dẫn exposures của dbt](https://docs.getdbt.com/docs/build/exposures).

## 8. Xử lý lỗi

| Lỗi | Cách kiểm tra |
|---|---|
| Thiếu env variable hoặc profile sai | Dùng đúng `--project-dir dbt --profiles-dir dbt` và nạp đúng `.env`; `dbt debug` để xác nhận |
| Connection refused | Docker Engine/warehouse phải chạy; host dùng 55434, container dùng service 5432 |
| `relation raw... does not exist` | Chạy ingestion qua Airflow trước; dbt không tạo nguồn raw |
| Freshness error | Mở `sources.json`, đối chiếu timestamp nguồn; không bỏ kiểm tra để làm run xanh |
| Model/test lỗi và downstream `SKIP` | Sửa resource lỗi đầu tiên; đọc `run_results.json`/`dbt.log`, rồi chạy lại phạm vi cần thiết |
| Sửa SQL nhưng task vẫn chạy code cũ | Rebuild runtime bằng bootstrap; DAG mount không tự mount toàn bộ code dbt |
| Backfill raw xong nhưng Silver chưa đổi | Dữ liệu có thể ngoài lookback incremental; rebuild nhánh cần thiết và đối soát theo ingestion guide |
