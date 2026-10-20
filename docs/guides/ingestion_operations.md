# Chạy và kiểm thử ingestion sau khi sửa

Ingestion đã triển khai transaction theo từng bảng, dedup theo watermark mới nhất,
staging riêng trong transaction, transaction advisory lock cho writer cùng bảng,
và backfill event theo khoảng thời gian. Neon + sync Actions giữ nguyên; [Airflow dùng chung](airflow_setup.md) là lab trên warehouse dev riêng, [Docker full public](docker_distribution_plan.md) làm ở chặng cuối.

Luồng dữ liệu và failure path hiện tại: [data flow](../architecture/data_flow.md).
Các việc chưa sửa: [remaining work](../architecture/remaining_work.md).

Bản nâng cấp mới nhất: [Gold upgrade](gold_upgrade.md).

## Cấu hình

- Neon: database đã được cấp sẵn; `POSTGRES_CREATE_DATABASE=false` (mặc định).
- `POSTGRES_SSLMODE=require` cho Neon. Workflow sync đã đặt giá trị này. Local PostgreSQL không bật TLS có thể dùng `prefer` như `.env.example`.
- Chỉ bật `POSTGRES_CREATE_DATABASE=true` nếu chủ động muốn script tạo database, tài khoản có quyền và endpoint cho phép.
- `CLICKHOUSE_SECURE`: nếu bỏ trống thì suy từ port 443/8443; nguồn port 80 hiện tại dùng `false`.
- `INGESTION_LOOKBACK_SECONDS=7200`: mặc định 2 giờ, khớp Silver hiện tại. Watermark phải là epoch giây dạng số.
- `.env` được đọc từ root repository, không phụ thuộc working directory. Biến môi trường shell vẫn được ưu tiên.

Không cần thay tên các secret GitHub hiện tại. Password PostgreSQL được xử lý bằng `URL.create`.

## Chạy bình thường

Từ root, sau khi đã kiểm tra đúng database đích:

```powershell
.\venv\Scripts\Activate.ps1
python ingestion/clickhouse_to_postgres.py
```

Dimension rỗng hoặc schema thêm/bớt cột sẽ làm job fail và giữ nguyên bảng đó.
Nếu insert dimension lỗi sau TRUNCATE, cả transaction rollback và view phụ thuộc vẫn tồn tại.
Các bảng đã commit trước đó không rollback cùng bảng thất bại; retry cả ingestion trước khi chạy dbt.

Event cùng ID chọn timestamp mới nhất. Payload khác nhau nhưng cùng timestamp mới nhất sẽ fail;
cần đối chiếu version tại nguồn, không chọn ngẫu nhiên. Batch cũ không ghi đè version mới hơn đã có.
Staging tạo/drop trong cùng transaction nên lỗi không để bảng staging mới bị commit dở dang.

Lock chỉ bảo vệ các writer sử dụng cùng quy tắc; workflow concurrency bảo vệ các run GitHub.
Không chạy laptop và GitHub/Airflow cùng lúc trên cùng warehouse trong khi dbt đang build.

## Backfill event

```powershell
python ingestion/clickhouse_to_postgres.py --start "2026-10-01T00:00:00+07:00" --end "2026-10-02T00:00:00+07:00"
```

Khoảng là `[start, end)` theo server timestamp, cần cả hai tham số và timezone.
Backfill bỏ qua MAX hiện tại của đích. Các dimension vẫn được refresh về trạng thái hiện tại;
đây chưa phải snapshot dimension lịch sử.

Sau khi backfill raw cũ hơn lookback Silver, Silver sẽ không tự nhận hết chỉ với incremental build.
Cần rebuild/backfill Silver và downstream phù hợp; thử trong dev trước. Ví dụ rebuild từ raw hiện có:

```powershell
python -m dotenv -f .env run -- dbt build --full-refresh --select silver_user_event_tracking+ --project-dir dbt --profiles-dir dbt
```

Lệnh có ghi/thay dữ liệu. Không chạy tự động sau mọi ingestion. Full-refresh dbt không tự trích xuất lại ClickHouse.

## Test offline và PostgreSQL local

```powershell
python -m unittest discover -s tests -v
```

Không có `TEST_DATABASE_URL`, unit tests vẫn chạy và integration tests sẽ skip.
Muốn kiểm tra rollback và SQL thật, dùng PostgreSQL local riêng:

```powershell
docker run --name laplaptech-test-pg -e POSTGRES_PASSWORD=local_test_only -e POSTGRES_DB=laplaptech_pipeline_test -p 127.0.0.1:55432:5432 -d postgres:16
docker exec laplaptech-test-pg pg_isready -U postgres -d laplaptech_pipeline_test
$env:TEST_DATABASE_URL = 'postgresql+psycopg2://postgres:local_test_only@localhost:55432/laplaptech_pipeline_test'
python -m unittest discover -s tests -v
```

Chờ DB ready trước khi chạy. Test chỉ chấp nhận localhost và database `laplaptech_pipeline_test`.
Mỗi test tạo schema ngẫu nhiên và xóa đúng schema đó khi xong; không dùng schema raw trên Neon.

Bộ test kiểm tra rollback sau TRUNCATE, giữ view, replay batch, batch cũ/mới,
xung đột cùng timestamp, rollback sau DELETE, dọn staging, concurrent writers,
watermark dạng numeric/Decimal, backfill và daily KPI sát nửa đêm Việt Nam.

## Tích hợp với dbt và workflows

- Giữ mô hình daily KPI mới theo session-day và bổ sung chú thích grain + hai singular tests đối soát event/rate.
- Giữ trigger main và concurrency của workflow sync; bổ sung guard cho manual dispatch, freshness và lưu artifacts.
- Workflow validate cài `requirements.txt` trỏ tới `requirements.lock.txt`, dùng lock làm cache key; chạy test với PostgreSQL service riêng.
- Raw và Silver có thêm test khóa event. Các test dữ liệu chạy thật trong `dbt build`; parse không xác nhận dữ liệu trên Neon.
- Quyết định kiến trúc và lịch sử sửa nằm ở [upgrade plan](../architecture/upgrade_plan.md); trạng thái hiện tại được tách khỏi ghi chép Sprint 1–3.

Dependencies runtime đã khóa 85 package và cài sạch trên Windows/Linux. Dashboard đã sửa Today, session-days, N/A, literal search và sử dụng đủ 12 Gold model; xem Gold upgrade.

## Kết quả kiểm chứng ngày 19/10/2026

- 28/28 Python test pass: 17 unit/helper tests, 9 PostgreSQL integration và 2 Streamlit AppTest.
- Có kiểm tra rollback sau TRUNCATE/DELETE, view, hai writer đồng thời và SQL KPI thật.
- 31 model build thành công và 47 data test pass trên dev, với fixture và nguồn ClickHouse thật 784.455 event. Replay không đổi row counts.
- YAML và Python compile pass; `pip check` không phát hiện dependency hỏng.
- Đã đọc ClickHouse thật vào dev local và chạy build/replay; không ghi thay đổi vào Neon. Report nằm trong docs/validation/.
