# Ingestion: chạy thủ công và backfill

**Dùng khi:** cần chạy lại một khoảng event, kiểm watermark hoặc điều tra raw. Lượt thường ngày đã do Airflow chạy; [sơ đồ ingestion](../architecture/data_flow.md#2-ingestion-dữ-liệu-vào-raw-như-thế-nào) giải thích trước khi thao tác.

## 1. Mục đích và điều kiện

Luồng thường ngày chạy qua Airflow. Hướng dẫn này dành cho điều tra lỗi hoặc nạp lại một khoảng event; [Docker guide](docker.md) hướng dẫn cài stack và đăng nhập, [data flow](../architecture/data_flow.md) giải thích transaction, watermark và failure path.

Source: [`clickhouse_to_postgres.py`](../../ingestion/clickhouse_to_postgres.py). Ingestion snapshot 5 bảng và dedup/upsert event theo ID/version. Mỗi bảng commit riêng; cả pipeline không phải một transaction.

## 2. Chuẩn bị môi trường và target

Chạy từ **root repo** trong PowerShell, với Docker stack đã hoạt động. Nếu chưa có Python environment, tạo theo [dbt guide, mục 4](dbt.md#4-dùng-dbt-cli-trên-máy-để-học-và-sửa-model). Dùng đúng Python 3.13 đã cài `requirements.txt`:

```powershell
$python = '.\.venv\Scripts\python.exe'
$dbt = '.\.venv\Scripts\dbt.exe'
& $python -m ingestion.clickhouse_to_postgres --help
```

| Cấu hình | Giá trị của hệ thống hiện hành |
|---|---|
| PostgreSQL trên host | `127.0.0.1:55434/laplaptech_pipeline_dev`, user `laplaptech_pipeline` |
| PostgreSQL trong task | `warehouse-laplaptech:5432/laplaptech_pipeline_dev` |
| SSL / tạo DB | `POSTGRES_SSLMODE=prefer`, `POSTGRES_CREATE_DATABASE=false`; Compose tạo DB |
| Lookback Raw và Silver | Cùng `INGESTION_LOOKBACK_SECONDS=7200`, mỗi tầng có MAX timestamp riêng |
| Source | `CLICKHOUSE_*` trong root `.env`; nguồn công khai dùng HTTPS với `CLICKHOUSE_PORT=443` và `CLICKHOUSE_SECURE=true` |

Trước mọi lệnh ghi, pause `laplaptech_pipeline` trong Airflow và đợi hết queued/running runs. Kiểm tra root `.env` trỏ đúng warehouse, không chạy writer khác cùng lúc. Các lệnh dùng dotenv `--override` để nạp rõ file này, tránh biến PostgreSQL cũ trong shell.

Runtime Airflow dùng config volume riêng và guard host/database/user; sửa root `.env` không tự cập nhật volume. Đổi credential nguồn phải chạy lại bootstrap theo [Docker guide](docker.md).

## 3. Chạy ingestion thông thường

Với các điều kiện mục 2 đã đạt:

```powershell
& $python -m dotenv -f .env run --override -- $python -m ingestion.clickhouse_to_postgres
if ($LASTEXITCODE -ne 0) { throw 'Ingestion failed; inspect the first error before building dbt.' }
```

**Checkpoint:** năm snapshot và batch event commit thành công. Nếu một bảng lỗi, xử lý lỗi và replay ingestion trước khi tiếp tục dbt.

- Snapshot rỗng hoặc thay tập tên cột bị từ chối. TRUNCATE và insert dùng cùng transaction; lỗi insert giữ lại dữ liệu cũ và view phụ thuộc.
- Bảng đã commit trước lỗi vẫn giữ dữ liệu mới. Retry không rollback toàn bộ lượt ingestion.
- Event chọn timestamp lớn nhất theo ID; version cũ không thay version mới. Cùng ID/timestamp mới nhất nhưng payload khác sẽ fail để điều tra nguồn.
- Staging UUID và advisory lock bảo vệ writer tuân theo cùng quy tắc. Pool Airflow không chặn CLI thủ công ngoài DAG.

Sau ingestion, dùng [dbt guide](dbt.md) để chạy freshness/build và đối soát trước khi unpause DAG.

## 4. Backfill event theo khoảng thời gian

`Invoke-LocalRecovery.ps1` chạy `ingest [start,end) → freshness → dbt build --full-refresh → reconcile` trên warehouse Docker. Chọn mốc theo **server-received timestamp**, nhập hai timestamp ISO-8601 có timezone. Khoảng bao gồm `start` và loại `end`. Năm bảng catalog vẫn refresh theo snapshot hiện tại, nên recovery chạy toàn bộ graph.

1. Ghi lại trạng thái pause, khoảng cần phục hồi và các ID/version mong đợi. Dùng [quy trình backup](docker.md#7-artifacts-và-backup) trước lượt ghi lịch sử.
2. Đợi các lượt Airflow hoàn tất, pause `laplaptech_pipeline`, kiểm tra toàn platform đã drain và không còn CLI writer. Dừng scheduler bằng `docker stop --timeout 60 airflow-platform-airflow-scheduler-1`. Giữ metadata và warehouse chạy. Lịch của mọi DAG dùng chung sẽ tạm dừng trong cửa sổ này.
3. Dùng PowerShell từ root repo. Lệnh đọc registry và audit tài nguyên trước khi chạy. Dùng Python đã có trên host, hoặc truyền `-Python` với đường dẫn Python trong venv.

   ```powershell
   .\scripts\Invoke-LocalRecovery.ps1 -Start '2026-10-01T00:00:00+07:00' -End '2026-10-02T00:00:00+07:00'
   ```

4. Đọc `recovery.json` và report từng stage tại đường dẫn `/artifacts/...` in ra, trong volume `laplaptech-lab-artifacts`. So sánh counts/watermark trước–sau, số ID tải từ nguồn, số bản cũ được giữ ngoài ghi đè, kết quả dbt test và đối soát Gold. `written_ids` gồm cả ID nạp lại, nên không dùng số này như số event mới. Kiểm tra ID/version cần khôi phục nếu nguồn còn có dữ liệu, rồi kiểm dashboard đọc Gold.
5. Sau khi đã kiểm chứng, chạy `docker start airflow-platform-airflow-scheduler-1` và khôi phục đúng trạng thái pause trước đó. Nếu lỗi, đọc report của stage đầu tiên thất bại và giữ LapLapTech paused trong lúc xử lý. Có thể chạy lại cùng khoảng sau khi sửa lỗi.

Recovery có khóa phiên để chặn hai lượt recovery đồng thời. Giữ cửa sổ bảo trì cho cả các writer CLI khác. Mỗi stage/model vẫn có transaction riêng, nên lỗi giữa chừng có thể để lại một phần dữ liệu đã commit. Freshness vẫn chạy theo quy tắc hiện có; nếu nguồn/Raw đã quá cũ, lệnh dừng trước build và giữ report ingest.

## 5. Kiểm thử và xử lý lỗi

Các bước tạo PostgreSQL test riêng, fixture full build/replay, rollback/concurrency tests và dashboard AppTest nằm trong [testing guide](testing.md). Không dùng fixture/reset trên warehouse đang vận hành.

| Triệu chứng | Hành động |
|---|---|
| Đọc watermark lỗi | Kiểm tra kiểu cột, kết nối và lỗi đầu tiên; không đổi lỗi thành full extraction |
| Schema snapshot đổi | So sánh tên cột nguồn/đích, thiết kế migration trước khi nạp lại |
| Cùng ID/timestamp nhưng payload khác | Đối chiếu version nguồn; không chọn ngẫu nhiên một bản |
| Task exit 137 / hết RAM | Đo volume nguồn/RAM; chunk insert không làm extraction thành streaming |
| Raw có event nhưng Silver thiếu sau backfill | Kiểm tra watermark riêng của Silver và rebuild nhánh cần thiết |

Lookback hữu hạn không bắt mọi bản nguồn có timestamp quá cũ. Raw không lưu immutable history và chưa đồng bộ source delete bằng CDC. DAG chưa triển khai bounded historical replay. Xem [giới hạn và hướng nâng cấp](../architecture/data_flow.md).

Các kết quả đã chạy được liệt kê theo ngày trong [testing guide](testing.md#8-báo-cáo-kiểm-chứng-theo-ngày); không dùng con số lịch sử làm trạng thái realtime.

CLI trên host không chịu Docker cgroup hay pool Airflow. Với dữ liệu thật, ưu tiên task Docker theo [resource policy](resources.md); khi debug CLI cần drain writer và dành đủ RAM cho tiến trình host. Chunk insert không biến extraction DataFrame thành streaming.
