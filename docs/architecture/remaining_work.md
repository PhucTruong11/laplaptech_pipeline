# Những việc còn lại sau khi sửa ingestion

Rà soát source ngày 19/10/2026. Đây là backlog; các checkbox chưa đánh dấu nghĩa là chưa triển khai hoặc chưa có bằng chứng hoàn tất. Không chạy thêm ingestion/dbt build lên Neon trong lần cập nhật tài liệu này.

## 1. Ưu tiên trước lần demo tiếp theo

### P1 — Dashboard phải khớp metric mới

File: [streamlit_app/app.py](../../streamlit_app/app.py).

- [ ] Today dùng ngày hiện tại Việt Nam thay vì `MAX(event_date)`; hiển thị ngày dữ liệu gần nhất.
- [ ] All-Time: đổi nhãn SUM daily sessions thành session-days hoặc query distinct session đúng phạm vi.
- [ ] Đổi nhãn detail views/comparisons khi số đang đếm session có hành vi, không phải event count.
- [ ] Bỏ fallback pin `8.8`; benchmark thiếu thì N/A, không thay bằng kết quả đo giả.
- [ ] `str.contains(..., regex=False)` để search ký tự `[` không làm lỗi app.
- [ ] `ORDER BY search_volume DESC, search_keyword` trước `LIMIT 25`; top keyword chưa đủ chứng minh product gap.
- [ ] Monthly total traffic lấy daily KPI hoặc ghi rõ chỉ là CPU detail views đã lọc nhóm >= 5.
- [ ] Brand share tính toàn bộ hoặc thêm Other; hiện donut chỉ hiển thị một phần brand.

Nghiệm thu: fixture có ngày cũ, session qua nửa đêm, benchmark NULL, lỗi truy vấn và search `[` đều có kết quả/nhãn đúng. Không coi không có dữ liệu là lỗi kết nối hoặc ngược lại.

### P1 — Đồng bộ timezone và grain giữa các mart

Files: [session funnel](../../dbt/models/silver/silver_session_funnel.sql), [comparison session-device](../../dbt/models/silver/silver_comparison_session_device.sql), [CPU trend](../../dbt/models/gold/mart_cpu_trend.sql), [GPU trend](../../dbt/models/gold/mart_gpu_trend.sql) và các view cast timestamp sang date.

- [ ] Quy định ngày server nhận theo giờ Việt Nam cho các báo cáo nghiệp vụ cần cùng calendar.
- [ ] Ghi rõ session-start cohort của funnel khác daily active session của KPI.
- [ ] Giữ nhãn step reach; chỉ xây sequential funnel nếu muốn đo thứ tự thật.

Nghiệm thu: event quanh 17:00 UTC và một session qua hai ngày có kết quả theo định nghĩa đã chọn. Không ép hai metric khác grain phải bằng nhau.

### P1 — Xác nhận end-to-end trên dev

- [ ] Cài dependencies trên môi trường sạch; chạy toàn bộ build và 34 data test với dataset dev đủ schema.
- [ ] Đối soát ID/version raw-Silver, số event/ngày và row counts trước/sau replay.
- [ ] Chạy backfill một khoảng cũ; xác nhận Silver/downstream được cập nhật khi ngoài lookback.
- [ ] Lưu run_results/sources/manifest mới; không coi 34 test đã parse là 34 test pass.

Đây là phần kiểm chứng còn thiếu, không phải kết luận pipeline mới chắc chắn lỗi trên Neon. Ưu tiên database dev/local hoặc nhánh dữ liệu riêng trước warehouse đang phục vụ.

## 2. Ưu tiên củng cố tính đúng và khả năng tái lập

| Việc | Bằng chứng hiện tại | Tiêu chí hoàn thành |
|---|---|---|
| Khóa dependency | `requirements.txt` chưa pin; CI cài mới mỗi lượt | Lock/constraints phù hợp Linux và Windows, cài sạch + test pass |
| Raw ID constraint/index | Loader có dedup/lock; chưa có migration unique index | Kiểm tra dữ liệu cũ, thêm NOT NULL/unique ID và index watermark có chủ đích |
| JSON và ID không hợp lệ | Silver cast `::json`; device_id chỉ kiểm tra regex trước cast int | Chính sách fail/quarantine rõ; JSON lỗi và số vượt int không làm mất dấu dữ liệu |
| Raw-Silver reconciliation | Đã có test ID từng bảng và event count của Gold | Đối soát ID, version, rejected rows giữa nguồn/raw/Silver |
| First run không có event | Loader bỏ qua batch rỗng, có thể chưa tạo raw event table | Chọn schema bootstrap có kiểu rõ hoặc fail sớm có thông báo phù hợp |
| Audit run | Log và artifact chưa có load heartbeat trong DB | Có run_id, started/finished/status, row counts và watermark; phân biệt nguồn ít traffic với job không chạy |
| Publish Gold nhất quán | dbt materialize từng model; app query nhiều table | Ghi nhận run/version phục vụ hoặc publish tập mart sau khi tests đạt nếu cần |

Không cần thêm streaming hoặc hệ thống phân tán để giải quyết các mục này.

## 3. Những định nghĩa phân tích cần chốt thêm

- [ ] **Brand comparison rate:** [mart_brand_interest](../../dbt/models/gold/mart_brand_interest.sql) chia hai tập session được đếm độc lập. Tử số không buộc thuộc tập detail-view ở mẫu số, nên có thể vượt 100%; chọn co-reach hoặc định nghĩa tỷ lệ khác.
- [ ] **Cặp brand:** [mart_brand_comparison](../../dbt/models/gold/mart_brand_comparison.sql) sắp xếp theo laptop ID, chưa canonicalize brand ID; cùng hai brand có thể xuất hiện ở hai hướng. Chuẩn hóa cặp trước GROUP BY nếu phân tích cặp không có hướng.
- [ ] **So sánh thực tế:** model comparison hiện suy từ device_id cùng xuất hiện trong session, chưa chứng minh hai laptop cùng một lần so sánh. Chốt semantics của device_ids/event trước khi dùng từ conversion/comparison action.
- [ ] **Benchmark grain:** ranking/battery/efficiency join theo laptop ID; chưa có test một benchmark/laptop ở Silver. Nếu nhiều benchmark hợp lệ, chọn latest/aggregate hoặc giữ grain laptop-benchmark rõ ràng để tránh fanout.
- [ ] **OS của session:** [silver_session_activity](../../dbt/models/silver/silver_session_activity.sql) dùng MAX chuỗi, không phải first non-null như comment; chọn quy tắc đại diện và sửa SQL/comment tương ứng.
- [ ] **Exposure:** khai báo 12 mart trong khi UI query 8; cập nhật dependency thực tế hoặc bổ sung view UI có chủ đích.

Đây là các khả năng sai nghĩa/fanout dựa trên code, chưa phải số liệu đã xác nhận xảy ra trong dataset thật.

## 4. Airflow cho mục tiêu học tập

- [ ] Docker image/runtime, metadata PostgreSQL riêng, mount repo và quản lý env.
- [ ] DAG `ingestion -> source freshness -> dbt build`, bắt đầu manual trên dev.
- [ ] Retry, timeout, max_active_runs và lưu log/artifact; test khi task fail.
- [ ] Nối data interval với `--start`/`--end`; xử lý Silver ngoài lookback trước khi bật catchup.
- [ ] Khi chuyển lịch chính: dừng lịch/run ghi dữ liệu trong Actions trước; giữ Actions cho CI.

Không bắt buộc VPS cho lab. Tắt laptop/Docker thì lab không chạy lịch. Docker và việc host warehouse trên Neon là hai quyết định riêng.

## 5. Thứ tự đề xuất

1. Dashboard và timezone/grain để demo giải thích đúng số liệu.
2. Dependency + một lượt end-to-end dev có bằng chứng; thêm reconciliation quan trọng.
3. Airflow Docker để học điều phối trên nền ingestion đã có test.
4. Constraint/index, audit/quarantine và làm rõ các mart nâng cao theo tình huống học.

Luồng đang có code: [data flow](data_flow.md). Cách chạy: [ingestion operations](../guides/ingestion_operations.md). Code mẫu cho phần chưa sửa: [pipeline fix guide](../guides/pipeline_fix_guide.md).
