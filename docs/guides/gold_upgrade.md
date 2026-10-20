# Gold, metrics, dependency lock và dashboard — 19/10/2026

Trạng thái mới nhất: **31 model, 47 dbt data test pass** trên fixture và dữ liệu ClickHouse thật được nạp vào PostgreSQL dev local. **28 Python test pass**, gồm ingestion integration và Streamlit AppTest. Không chạy ghi dữ liệu vào Neon trong đợt nâng cấp.

Bằng chứng máy đọc được: [dev run report](../validation/dev_run_2026-10-19.json). Report không chứa password hoặc payload người dùng. Chi tiết nguồn/raw vẫn nằm ở database dev, không commit vào repository.

## 1. Convention tên và migration

12 model trong `dbt/models/gold/` đổi từ `mart_*` sang `gold_*`. Đây là convention của project, không phải yêu cầu bắt buộc của dbt. File model, `ref()`, tests, exposure, dashboard và tài liệu dùng tên mới.

Schema mặc định vẫn là `public_gold`; ví dụ relation mới `public_gold.gold_daily_site_kpis`.

dbt tạo relation tên mới; **không tự rename hoặc drop các bảng `mart_*` cũ trên Neon**. Chuyển đổi khi bạn sẵn sàng deploy:

1. Dừng các lượt ghi trùng warehouse; kiểm tra đúng cấu hình database đích.
2. Cài `pip install -r requirements.txt`, hiện trỏ tới lock đầy đủ.
3. Kiểm tra source freshness. Silver có thêm taxonomy/cột và logic mới, nên cần một lần rebuild toàn bộ lịch sử từ raw:

```powershell
python -m dotenv -f .env run -- dbt source freshness --project-dir dbt --profiles-dir dbt
python -m dotenv -f .env run -- dbt build --full-refresh --project-dir dbt --profiles-dir dbt
```

4. Chỉ mở dashboard mới sau khi `gold_*` build/tests đạt. Chạy incremental thường không cập nhật taxonomy của các row Silver nằm ngoài lookback.
5. Bảng `mart_*` cũ có thể được giữ tạm để so sánh/rollback. Chỉ dọn riêng sau khi kiểm tra không còn consumer dùng; đợt sửa này không chạy cleanup trên Neon.

Full-refresh có thể cần thêm dung lượng tạm cho relation cũ/mới. Kiểm tra quota/dung lượng của Neon trước khi tự chạy; lần kiểm chứng ở đây dùng PostgreSQL local, không xác nhận sức chứa warehouse cloud.

## 2. Timezone và contract metric

`dbt_project.yml` khai báo `business_timezone: Asia/Ho_Chi_Minh`. Macros `business_date` và `business_month` được dùng cho ngày/tháng nghiệp vụ. Timestamp gốc vẫn giữ timezone; `silver_session_activity.session_start` giữ cả giờ, có thêm `session_start_date` riêng.

| Gold model | Grain và metric |
|---|---|
| `gold_daily_site_kpis` | Ngày Việt Nam; event count, pageview count, detail/comparison pageviews, active session-days, co-reach numerators và latest event timestamp |
| `gold_behavior_funnel_daily` | Cùng session-day với daily KPI; các bước đạt độc lập, không yêu cầu thứ tự |
| `gold_brand_interest` | Brand, session toàn thời gian; detail pageviews và comparison interactions được quy về catalog |
| `gold_brand_comparison` | Cặp brand ID không có hướng; co-occurrence của comparison interactions trong cùng session-day |
| `gold_cpu_trend` / `gold_gpu_trend` | Tháng Việt Nam + component + brand; detail pageviews, không còn loại nhóm dưới 5 view |
| `gold_spec_popularity` | CPU/GPU/screen/battery; detail views, chưa có RAM/storage |
| `gold_search_analytics` | Keyword lower/trim; search event count và distinct session |
| `gold_user_os` | OS/device type đầu tiên không rỗng theo event time/ID trong mỗi session; có Unknown |
| `gold_performance_ranking` | Laptop active, một benchmark đại diện |
| `gold_performance_efficiency` | Laptop active/visible có benchmark; chia thiếu/bằng 0 trả NULL |
| `gold_battery_vs_interest` | Laptop active; benchmark, detail views và comparison interactions |

Benchmark đại diện là **benchmark ID lớn nhất mỗi laptop** vì nguồn chưa có timestamp/version đáng tin cậy được dùng cho lựa chọn này. Không gọi đó là benchmark mới nhất theo thời gian. Raw/Bronze vẫn giữ mọi benchmark; Silver chọn một bản để tránh fanout. Khi có policy nghiệp vụ tốt hơn, đổi lựa chọn và kiểm thử lại.

Daily rates cộng tử/mẫu rồi mới chia, không lấy trung bình các phần trăm theo ngày. Nhiều ngày SUM active sessions là **session-days**, không phải unique session toàn thời gian. Daily KPI và all-time brand/OS có grain khác nhau, không ép các tổng sessions bằng nhau.

## 3. Taxonomy được xác nhận trên nguồn thật

Nguồn có các trang `DeviceComparisonHome`/`DeviceComparison`, không chỉ tên `CompareDevice` trong model cũ. Silver giữ `source_page_name` và chuẩn hóa `page_name=CompareDevice` cho hai trang này và các chart comparison events.

- Selection: `select_device_for_comparison`, `add_to_comparison` (vẫn hỗ trợ click/DeviceDetail của fixture/legacy).
- Sort: `comparison_chart_sort_selection`, hoặc click/CompareDevice có sort_by.
- Device arrays: `device_ids` được mở rộng qua `silver_comparison_event_device`; loại ID trùng trong cùng event và ID ngoài khoảng integer hợp lệ.
- Comparison interactions gồm selection/add/chart events và comparison pageviews có device ID. Không khẳng định mọi cặp cùng một lần so sánh đồng thời.
- OS đọc `device.os`, fallback `device.os_name`; chọn first non-empty theo timestamp/ID cho session.

`silver_session_day` là nguồn chung cho daily KPI và funnel. `silver_comparison_event_device` là bridge chung cho attribution brand/product và comparison session-device. Các generic/singular tests đối chiếu raw → Silver, taxonomy, Gold totals/rates và uniqueness.

## 4. Lock dependency có thể cài lại

- `requirements.in`: dependency trực tiếp cần dùng.
- `requirements.lock.txt`: 85 version runtime đã pin, gồm transitive closure; không kéo cả Jupyter/dev tooling vào runtime.
- `requirements.txt`: `-r requirements.lock.txt` để giữ nguyên lệnh cài hiện có.
- `scripts/lock_dependencies.py`: ghi lại closure từ môi trường đã resolve và test, đánh giá marker Windows/Linux Python 3.13. Script này không tự resolve version mới.

Đã cài sạch và `pip check` pass trên Windows venv Python 3.13 và Linux `python:3.13-slim`. E2E dùng venv Windows mới + PostgreSQL Docker; không tuyên bố đã chạy toàn bộ E2E trên Linux. CI dùng lock, PostgreSQL fixture, build/replay và AppTest cho các lần tiếp theo.

Khi cập nhật dependency: sửa intent ở `.in`, resolve/cài trong môi trường riêng, chạy script khóa, cài sạch cả hai nền tảng và kiểm thử lại. Lock version chưa bao gồm hash của artifact phân phối.

## 5. Chạy dev tái lập

Tạo PostgreSQL local riêng như hướng dẫn ingestion; database mặc định cho fixture là `laplaptech_pipeline_dev`, password chỉ dành cho local ví dụ. `DEV_DATABASE_URL` chỉ chấp nhận localhost và ba database dev/test đã allowlist.

```powershell
# Ví dụ khi PostgreSQL dev local được bind port 55432.
$env:DEV_DATABASE_URL = 'postgresql+psycopg2://postgres:local_dev_only@localhost:55432/laplaptech_pipeline_dev'
python -m scripts.run_dev_pipeline --source fixture
```

Runner dùng extraction/load thật, chỉ thay source client bằng fixture. Nó chạy freshness → full build → ingestion replay → incremental build → so sánh row counts. Fixture có duplicate, missing session, ID quá lớn, ranh giới ngày/tháng Việt Nam, source taxonomy, array trùng ID và nhiều benchmark/laptop. Có reference Python từ raw kiểm tra event/session-day theo timezone độc lập với macro SQL.

Khi thay contract fixture trên database đã có dữ liệu, có thể dùng `--reset-fixture`; flag này chỉ drop/recreate bốn schema pipeline của database fixture local và bị chặn ở dev-live. Không dùng cho dữ liệu nghiên cứu muốn giữ lại.

Để đọc nguồn ClickHouse thật vào dev, tạo database local riêng `laplaptech_pipeline_dev_live`, đổi URL sang đó rồi chạy:

```powershell
python -m scripts.run_dev_pipeline --source clickhouse
```

CLI đọc ClickHouse theo credential root `.env`, nhưng PostgreSQL/dbt luôn được override về URL local đã kiểm tra. `--allow-stale-source` chỉ dành cho historical dev và ghi rõ freshness fail trong report; không dùng nó để đổi policy workflow production. Lần chạy thật được ghi nhận ở đây freshness pass.

Output: `scratch/dev-e2e-<UTC timestamp>/report.json`, `target/` và `logs/`. Không commit raw extract. CI upload report/manifest/run_results/sources làm bằng chứng.

## 6. Dashboard mới

5 tab dùng đủ 12 Gold model: Overview, Brands & hardware, Behavior, Performance, Catalog.

- Overview: Today/7 days/All available/Custom theo ngày Việt Nam, event/pageview counts thực tế, session-days đúng nhãn và rates có denominator rõ.
- Hiển thị latest received event và độ trễ event; không gọi đó là thời điểm pipeline hoàn tất.
- Refresh data xóa query cache 300 giây. Query lỗi là error, không biến thành KPI 0.
- Brand share giữ cả Other; hardware không bỏ low-volume groups. Search query order rõ, không gán nhãn product gap khi chưa có zero-result data.
- Missing benchmark là N/A/NULL; không còn pin giả 8.8 hay zero thay measurement.
- Catalog literal search, punctuation không phải regex; join được validate one-to-one.
- Giữ protection mặc định của Streamlit; bỏ cấu hình tắt CORS/XSRF từ bản cũ.

`DASHBOARD_DATABASE_URL` override riêng cho preview/dev; `GOLD_SCHEMA` mặc định `public_gold`. Nếu không override, app đọc cấu hình PostgreSQL root `.env` như trước.

Preview local của lần làm việc này: [dashboard](http://127.0.0.1:8502). Môi trường local đang được giữ để tiếp tục; URL chỉ hoạt động khi process và Docker còn chạy. Cấu hình tiếp tục nằm trong `scratch/dev.env` (ignored), không chứa credential Neon.

Screenshot dữ liệu thật trên dev: [dashboard preview](../../assets/dashboard_preview.png).

## 7. Phần chưa làm

Chưa triển khai Airflow, audit table/job heartbeat, quarantine JSON lỗi, raw ID constraint migration hoặc publish Gold atomically theo run. Báo cáo trong hướng dẫn này thuộc lần kiểm chứng dev; chủ project xác nhận đã chạy trên Neon. Dữ liệu danh mục hiện tại vẫn được join vào lịch sử event; chưa có SCD snapshot lịch sử.

Kế hoạch sau Gold upgrade: [Airflow dùng chung](airflow_setup.md) → [engineering](../architecture/remaining_work.md) → [Docker full public ở chặng cuối](docker_distribution_plan.md). Neon và lịch Actions hiện tại được giữ nguyên.
