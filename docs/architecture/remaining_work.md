# Trạng thái công việc sau Gold upgrade — 19/10/2026

Đã kiểm chứng trên PostgreSQL dev local bằng fixture và nguồn ClickHouse thật 784.455 event. Neon chưa deploy. Chi tiết: [Gold upgrade](../guides/gold_upgrade.md), [bằng chứng](../validation/dev_run_2026-10-19.json).

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

## 2. Khi muốn đưa bản mới lên Neon

- [ ] Kiểm tra dung lượng và khoảng trống cho một lần full-refresh.
- [ ] Build `gold_*` và toàn bộ lịch sử Silver bằng implementation mới, kiểm tra tests/freshness.
- [ ] Sau khi build đạt mới mở dashboard mới; chưa tự drop bảng `mart_*` cũ.
- [ ] Xác nhận output trên môi trường cloud và lưu artifact mới.

Các bước này có ghi dữ liệu production nên chưa được thực hiện trong đợt dev. Hướng dẫn migration nằm trong Gold upgrade.

## 3. Những invariant và khả năng vận hành còn thiếu

| Việc | Hiện tại | Bước tiếp theo |
|---|---|---|
| Raw ID constraint/index | Loader lock/dedup, data tests đã có; chưa migration vật lý | Đối chiếu dữ liệu, thêm NOT NULL/unique ID và index watermark có chủ đích |
| JSON lỗi/quarantine | Payload hợp lệ được parse; JSON hỏng có thể fail model | Chốt fail/quarantine, lưu rejected rows và số lượng theo run |
| First run không có event | Empty batch có thể chưa tạo raw event table | Schema bootstrap có kiểu rõ hoặc fail sớm dễ hiểu |
| Audit/job heartbeat | Có report/log/artifact và event recency, chưa có run heartbeat trong DB | run_id, status, timing, row counts và watermark |
| Publish Gold nhất quán | Materialize từng table, chưa publish một version chung | Chọn serving snapshot/version nếu cần consistency theo run |
| Danh mục lịch sử | Historical event join vào catalog hiện tại | SCD/snapshot chỉ khi cần trả lời thay đổi theo thời gian |
| Benchmark version | Đại diện theo ID lớn nhất | Dùng timestamp/version thực khi nguồn cung cấp và nghiệp vụ xác nhận |
| Lock nâng cao | Version closure đã khóa và cài sạch hai nền tảng | Có thể thêm hashes/automated dependency updates nếu cần |

Brand pairs đo comparison-interaction co-occurrence trong session-day, không khẳng định một comparison action đồng thời. Funnel đo reach độc lập. Đây là giới hạn định nghĩa có chủ đích, không phải các tính năng sequential đã triển khai.

## 4. Airflow cho mục tiêu học tập

- [ ] Docker image/runtime và PostgreSQL metadata riêng.
- [ ] DAG ingestion → source freshness → dbt build, chạy manual trên dev trước.
- [ ] Retry, timeout, max_active_runs, lưu logs/artifacts và test failed task.
- [ ] Nối data interval với --start/--end; xử lý Silver ngoài lookback trước catchup.
- [ ] Khi chuyển lịch: dừng writer Actions trước, chỉ giữ Actions cho CI.

Chưa cần VPS cho lab nếu chấp nhận chỉ chạy khi laptop/Docker bật. Neon vẫn có thể giữ warehouse.

## 5. Thứ tự làm tiếp

1. Xem dashboard dev và metric contract để xác nhận semantics mong muốn.
2. Nếu tiếp tục học orchestration, làm Airflow Docker trên dataset dev đã được kiểm chứng.
3. Bổ sung audit/quarantine/constraint theo các tình huống lỗi thực hành.
4. Chỉ deploy Neon khi chủ động chọn chuyển phiên bản và đã kiểm tra dung lượng.

Luồng hiện tại: [data flow](data_flow.md). Commands: [Gold upgrade](../guides/gold_upgrade.md) và [ingestion operations](../guides/ingestion_operations.md).
