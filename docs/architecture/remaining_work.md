# Trạng thái công việc sau Gold upgrade — cập nhật 20/10/2026

Đã kiểm chứng trên PostgreSQL dev local bằng fixture và nguồn ClickHouse thật 784.455 event. Chủ project xác nhận đã chạy trên Neon; báo cáo trong repo vẫn là bằng chứng dev local. Chi tiết: [Gold upgrade](../guides/gold_upgrade.md), [bằng chứng](../validation/dev_run_2026-10-19.json).

## 1. Đã hoàn thành trong đợt này

- [x] Đổi 12 model `mart_*` sang `gold_*`, cập nhật refs/tests/exposure/UI.
- [x] Ngày/tháng nghiệp vụ dùng timezone Việt Nam; giữ timestamp session_start và thêm session_start_date.
- [x] Daily KPI và funnel dùng chung `silver_session_day`; reach không được gán nghĩa sequential conversion.
- [x] Chuẩn hóa taxonomy thực tế: DeviceComparison/Home, selection/add/sort events và os_name.
- [x] Bridge event-device mở rộng device_ids, loại ID trùng và ID vượt phạm vi integer.
- [x] Brand co-reach dùng giao của tập session; canonicalize cặp brand không có hướng.
- [x] Chọn một benchmark ID lớn nhất mỗi laptop để tránh fanout, ghi rõ đây chưa phải lựa chọn theo timestamp.
- [x] Không bỏ các nhóm CPU/GPU dưới 5 view; đối soát total theo phạm vi catalog.
- [x] Khóa 85 runtime package; cài sạch và pip check trên Windows/Linux Python 3.13.
- [x] Full build + incremental replay fixture và nguồn thật: 31 model, 47 data tests pass.
- [x] Đối soát raw → Bronze → Silver, raw/Silver ID-version, daily events/session-days và các Gold totals.
- [x] Fixture có reference Python độc lập cho ranh giới ngày/tháng, source taxonomy và benchmark fanout.
- [x] Dashboard 5 tab sử dụng đủ 12 Gold model; Today thật, session-days đúng nhãn, missing benchmark N/A, literal search.
- [x] Brand share giữ Other, search top-N có thứ tự, không suy luận product gap từ volume đơn thuần.
- [x] 28 Python tests pass, gồm 9 PostgreSQL integration và 2 dashboard AppTest.
- [x] CI có fixture end-to-end, replay, AppTest và upload bằng chứng.

## 2. Ranh giới vận hành đã chốt

| Môi trường | Điều phối | Đích ghi | Trạng thái |
|---|---|---|---|
| Đang vận hành | Sync GitHub Actions hiện tại | Neon hiện tại | Giữ nguyên lịch, host và cấu hình |
| Lab cá nhân | Platform Airflow 3.3.2 độc lập, dùng chung nhiều project | PostgreSQL dev riêng | Bộ cấu hình Airflow có sẵn; chờ nghiệm thu và tích hợp pipeline |
| Cộng đồng | Airflow standalone + runtime + dashboard trong Docker | Warehouse local của người clone | Chặng cuối sau nâng cấp engineering |

Cùng đọc ClickHouse public; mỗi warehouse có writer và cấu hình riêng. Bảng audit không phải điều kiện để bắt đầu lab Airflow. Không có bước tắt sync Actions hoặc migration lại Neon trong roadmap này. Báo cáo dev hiện có giữ nguyên phạm vi; chủ project đã xác nhận đang chạy trên Neon.

## 3. Chặng B — Airflow Docker dùng chung, làm trước

Hướng dẫn và code mẫu: [Triển khai Airflow](../guides/airflow_setup.md), [bộ cấu hình Airflow](../../infra/airflow-platform). Stack NASDAQ100 cũ đã gỡ; dựng workspace `airflow-platform` độc lập bằng Airflow 3.3.2/LocalExecutor. Một platform cho các project cá nhân.

- [x] Tạo Compose, env generator và DAG kiểm tra nền tảng Airflow 3; cập nhật hướng dẫn khởi tạo từ đầu.
- [x] Chủ project gửi bằng chứng init exit 0, services healthy và smoke DAG/hai task success ngày 20/10/2026.
- [ ] Kiểm tra logs/XCom chi tiết, retry/Clear và restart giữ trạng thái; không gộp vào bằng chứng DAG success.
- [x] Viết [bài tự triển khai DAG LaplapTech](../guides/airflow_laplaptech_walkthrough.md), có code mẫu và checkpoint; chưa tạo/bật các file tích hợp.
- [ ] Build image Airflow mở rộng với Docker provider tương thích 3.3.2 và kiểm tra `pip check` trước tích hợp.
- [ ] Image runtime LaplapTech Python 3.13 riêng, không cài lock pipeline đè lên environment Airflow.
- [ ] Network/config/artifact volumes và PostgreSQL dev riêng; preflight chặn đích ngoài lab trước lần ghi đầu.
- [ ] DAG `laplaptech_lab`: ingestion → source freshness → dbt build; manual, catchup tắt, run/task concurrency rõ.
- [ ] Pool riêng, task timeout/retry và giới hạn RAM được đo cho từng project.
- [ ] Lưu logs/artifacts theo run/task/attempt, không truyền dataset qua XCom.
- [ ] Manual run, replay fixture và lỗi/recovery có kết quả đối soát; DAG kiểm tra nền tảng vẫn chạy được.
- [ ] Sync Actions trên Neon và validate CI tiếp tục hoạt động như cấu hình hiện tại.

Nghiệm thu B: platform độc lập chạy DAG kiểm tra nền tảng và DAG LaplapTech; task LaplapTech chạy đúng runtime/warehouse dev; có bằng chứng build/test và phục hồi lỗi. Bộ cấu hình Airflow chỉ dựng control plane, chưa thực thi pipeline. Image task ở bước tích hợp vẫn chưa phải Docker full phát hành.

## 4. Chặng C — Các nâng cấp engineering sau Airflow

| Mục | Đầu ra cần triển khai | Tiêu chí nghiệm thu |
|---|---|---|
| C1 — Schema bootstrap | Schema/type rõ cho raw, gồm first run không có event | DB rỗng hoặc empty batch có kết quả xác định; không lỗi khó hiểu vì thiếu bảng |
| C1 — Raw constraint/index | Migration NOT NULL/unique ID và index watermark sau khi đối chiếu dữ liệu | Duplicate/version/null handling có test; replay và concurrent writer vẫn đúng; thử migration trên lab trước |
| C1 — JSON lỗi/quarantine | Chốt fail/quarantine, lưu reject reason và run ID khi chọn quarantine | Không bỏ row lỗi âm thầm; accepted/rejected counts đối soát được, metric có contract rõ |
| C2 — Run audit/heartbeat | `run_id`, stage/attempt, start/end, status, watermark và row counts có định nghĩa | Phân biệt job không chạy, chạy thành công không có event mới và task fail; retry không tạo success giả |
| C2 — Backfill thực sự | DAG nhận bounded interval có timezone, xử lý raw và Silver/Gold ngoài lookback | Đối soát ID/version và metric với fixture late data; không chỉ bật catchup để gọi lại MAX hiện tại |
| C2 — Recovery/artifacts | Quy tắc Clear/retry, retention, export logs/artifacts và backup/restore lab | Lỗi giữa chừng giữ đúng transaction boundaries; có thể đọc lại bằng chứng sau restart |

Thứ tự trong C: bootstrap/constraint/quarantine → audit/backfill/recovery. Audit theo run làm sau khi đã quan sát được DAG, không là điều kiện tiên quyết để học Airflow. Test fixture phải cô lập khỏi Neon; thay đổi logic ảnh hưởng cloud chỉ triển khai qua thay đổi riêng được kiểm chứng.

Các mục nâng cao có điều kiện:

| Mục | Khi nào cần |
|---|---|
| Publish Gold theo version/snapshot | Khi cần dashboard không thấy nhiều version mart từ một build dở dang |
| Streaming extraction/checkpoint | Khi đo được memory hoặc thời gian batch không phù hợp |
| SCD/snapshot danh mục | Khi câu hỏi phân tích cần catalog tại thời điểm lịch sử |
| Benchmark version theo timestamp | Khi nguồn có version/thời gian đáng tin cậy và policy được chốt |
| Hashes/automated dependency updates | Khi cần tăng mức tái lập và quy trình cập nhật dependency |

Đây không phải yêu cầu phải hoàn thành mọi hướng nghiên cứu trước khi phát hành Docker. Giữ giới hạn metric có chủ đích: funnel đo reach độc lập; brand pair đo co-occurrence session-day; benchmark đại diện theo ID lớn nhất.

## 5. Chặng D — Docker full cho cộng đồng, làm cuối

Kế hoạch chi tiết: [Docker distribution plan](../guides/docker_distribution_plan.md). Bắt đầu khi B và các mục cốt lõi C đã nghiệm thu.

- [ ] Đóng gói Airflow standalone chỉ có DAG LaplapTech, metadata DB, warehouse PostgreSQL local, runtime ingestion/dbt, Streamlit và init/bootstrap.
- [ ] Tái sử dụng CLI/image đã test; version matrix của runtime/control plane được pin và kiểm chứng.
- [ ] Cấu hình mẫu nguồn ClickHouse public, đích local; không yêu cầu credential Neon của chủ repo.
- [ ] Startup DB rỗng chạy pipeline đầu tiên, dashboard chỉ ready khi Gold/tests đạt; restart không reset dữ liệu.
- [ ] Volume/network/config riêng; không mount DAG project khác hoặc `.env` đang vận hành vào bản public.
- [ ] Quickstart clone sạch, nguồn thật, failure/replay và hướng dẫn dừng/restart/backup được test.
- [ ] Có số đo RAM/disk/thời gian initial load và ghi rõ nền tảng đã kiểm chứng.
- [ ] Neon, lịch sync Actions và validate CI vẫn giữ nguyên trong lúc kiểm chứng bản public.

Nghiệm thu D: người clone chỉ cần Docker/Compose và Internet để lấy nguồn public, có warehouse/dashboard/Airflow riêng; không phụ thuộc platform cá nhân, venv Windows hoặc tài khoản Neon của chủ repo. Lệnh quickstart chỉ ghi là chạy được sau khi bộ Docker thực tế đã được tạo và kiểm chứng.

## 6. Sprint tiếp theo

Chỉ lấy chặng B: copy bộ cấu hình Airflow ra workspace độc lập → init Airflow → nghiệm thu DAG kiểm tra nền tảng → chuẩn bị runtime/config/đích lab → thêm DAG LaplapTech → manual/replay/failure test. Chưa làm Docker full cho cộng đồng và chưa chuyển lịch cloud.

Tài liệu điều phối kế hoạch: [upgrade plan](upgrade_plan.md). Luồng đang chạy: [data flow](data_flow.md). Metric/CLI hiện có: [Gold upgrade](../guides/gold_upgrade.md), [ingestion operations](../guides/ingestion_operations.md).
