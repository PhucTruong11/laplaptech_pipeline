# LaplapTech Analytics Pipeline

Pipeline học Data Engineering: đọc ClickHouse public, nạp PostgreSQL, biến đổi bằng dbt và phục vụ phân tích qua Streamlit. Toàn bộ hệ thống chạy trên Docker local; Airflow điều phối ETL, GitHub Actions kiểm thử code bằng fixture.

## 1. Hệ thống hiện tại

| Thành phần | Cấu hình |
|---|---|
| Điều phối | Airflow 3.3.2 / LocalExecutor, DAG `laplaptech_pipeline` |
| Lịch | 08:15 và 16:45, timezone Việt Nam; một active run/writer |
| Warehouse | PostgreSQL 16, `127.0.0.1:55434/laplaptech_pipeline_dev` |
| Transformation | 31 dbt model: 6 Bronze views, 13 Silver relations, 12 Gold tables |
| Data quality | Source freshness, 47 dbt data tests và đối soát raw → Gold |
| Runtime | Python 3.13, 85 dependency được pin; task UID 10001 |
| Dashboard | Streamlit Docker, 5 tab sử dụng đủ 12 Gold model |
| CI | PostgreSQL fixture, full build/replay và Python/AppTest; không cần nguồn thật |

[Báo cáo hệ thống](docs/validation/system_check_2026-10-20.json) và [regression](docs/validation/regression_check_2026-10-20.json) ghi bằng chứng theo ngày/phạm vi, không xác nhận trạng thái realtime. Danh sách report ở [validation](docs/validation/README.md).

## 2. Data flow

![LaplapTech: ClickHouse → Airflow runtime → PostgreSQL → Streamlit](assets/data-flow.svg)

Airflow tạo các runtime container theo thứ tự `ingest → freshness → build → reconcile`. Python/dbt CLI chạy trong container; SQL transformation thực thi trong PostgreSQL. Metadata Airflow tách khỏi warehouse raw/Bronze/Silver/Gold. Dashboard đọc Gold và cache 300 giây.

Ingestion snapshot atomic theo từng bảng cho 5 bảng và dedup/upsert event theo ID/version với lookback 2 giờ. Freshness warn 24 giờ/error 48 giờ. Một task lỗi sẽ chặn downstream; Gold chưa có transaction publish chung cho toàn bộ model.

[Kiến trúc, grain, metric và failure path](docs/architecture/data_flow.md). Nguồn logo của sơ đồ được ghi tại [tool-logos.json](assets/tool-logos.json).

## 3. Chạy nhanh bằng Docker

Cần Git, PowerShell, Docker Desktop đang chạy **Linux containers** và credential nguồn ClickHouse public. Runtime đã đóng gói nên không cần tự cài Python/dbt/PostgreSQL trên host để chạy pipeline và dashboard. Hướng dẫn hiện hỗ trợ Windows Docker Desktop; [Docker guide](docs/guides/docker.md) ghi cấu hình RAM/cổng và phạm vi kiểm chứng.

Nếu chưa có repo, mở PowerShell tại thư mục muốn chứa project:

```powershell
git clone https://github.com/PhucTruong11/laplaptech_pipeline.git
if ($LASTEXITCODE -ne 0) { throw 'Clone failed; inspect Git output before continuing.' }
Set-Location -LiteralPath laplaptech_pipeline
```

Nếu đã clone thì chuyển vào **root repo** hiện có, không clone lồng thêm. Tiếp theo:

```powershell
if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
}
# Mở .env bằng editor, điền CLICKHOUSE_PASSWORD và lưu trước khi chạy.
.\scripts\Start-LocalStack.ps1
```

Bootstrap tạo/cập nhật workspace sibling `airflow-platform`, build images và nạp cấu hình nguồn. Keys Airflow hiện có được giữ; target task luôn là PostgreSQL Docker local. Không cần tài khoản Neon.

Dataset public vẫn yêu cầu login: `.env.example` đã có host/user/database, còn password phải lấy từ người duy trì dataset/repo. Nếu chưa có credential này thì chưa chạy được ingestion nguồn thật; [testing guide](docs/guides/testing.md) có lựa chọn fixture không cần login ClickHouse.

1. Mở [Airflow](http://localhost:8080), đăng nhập bằng `.env` của **platform**.
2. Unpause `laplaptech_pipeline`, Trigger với `full_refresh=true` khi cần rebuild Silver từ raw.
3. Đợi cả bốn task success, rồi mở [dashboard](http://localhost:8501).

Các lượt thường để `full_refresh=false`. Khi Docker/laptop tắt, lịch ETL và dashboard dừng; CI không chạy ETL thay Airflow. Named volumes giữ dữ liệu qua stop/recreate. Hướng dẫn đầy đủ và đăng nhập PostgreSQL: [Docker](docs/guides/docker.md).

## 4. Hướng dẫn theo công việc

[Mục lục tài liệu](docs/README.md) là điểm bắt đầu duy nhất.

| Bạn muốn làm gì? | Hướng dẫn |
|---|---|
| Setup, start/stop, kết nối DB, rebuild và backup | [Docker](docs/guides/docker.md) |
| Chạy DAG, đọc logs/Assets, hiểu retry và pool | [Airflow](docs/guides/airflow.md) |
| Run/test/build, source freshness, exposures và dbt docs | [dbt](docs/guides/dbt.md) |
| Điều tra ingestion, replay và backfill có kiểm soát | [Ingestion](docs/guides/ingestion.md) |
| Kiểm tra full system, fixture và regression/CI | [Testing](docs/guides/testing.md) |
| Hiểu Compose, Dockerfiles, mounts và volumes | [Cấu trúc hạ tầng](infra/README.md) |

## 5. Model và giới hạn

| Nhóm | Gold models |
|---|---|
| KPI và hành vi | `gold_daily_site_kpis`, `gold_behavior_funnel_daily`, `gold_user_os`, `gold_search_analytics` |
| Thương hiệu | `gold_brand_interest`, `gold_brand_comparison` |
| Cấu hình/xu hướng | `gold_cpu_trend`, `gold_gpu_trend`, `gold_spec_popularity` |
| Benchmark/pin | `gold_performance_ranking`, `gold_performance_efficiency`, `gold_battery_vs_interest` |

Daily KPI dùng ngày server nhận event theo giờ Việt Nam. Cộng daily sessions qua nhiều ngày là **session-days**. Funnel đo reach trong cùng session-day, chưa chứng minh thứ tự chuyển đổi. Search volume không tự chứng minh zero-result/product gap.

Raw không phải immutable history; lookback hữu hạn có thể bỏ sót bản nguồn có timestamp quá cũ. Extraction vẫn giữ DataFrame trong RAM. Bounded historical replay, quarantine và atomic Gold publication chưa được triển khai; xem giới hạn trong [data flow](docs/architecture/data_flow.md).

## 6. Kiểm tra hệ thống

Từ root repo, với Python 3.10+ và Docker CLI:

```powershell
python scripts/verify_local_stack.py --output scratch/system-check.json
```

Mặc định không trigger ETL. Thêm `--run-pipeline` để chạy một lượt ingestion/freshness/build/reconcile thật và xác nhận artifacts của đúng run. Fixture/reset chỉ dùng database `laplaptech_pipeline_test`; runner từ chối warehouse active trước khi kết nối hoặc ghi. Xem [testing](docs/guides/testing.md).

## Dataset attribution

Dataset contributed by **Nguyễn Ngọc Duy Luân (Duy Luân Dễ Thương)** to the [Xóm Data community](https://www.facebook.com/groups/xomdata). Used for educational analytics and portfolio development.

License: [MIT](LICENSE).
