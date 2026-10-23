# Roadmap cải tiến LapLapTech

Project **đã chạy được end-to-end trên Docker local**. Đây là danh sách việc tiếp theo, không phải chức năng đang có. Chỉ làm mục mới khi có nhu cầu hoặc số đo chỉ ra vấn đề; mỗi mục có tiêu chí kiểm chứng để tránh thêm tool cho đẹp hồ sơ.

| Ưu tiên | Vấn đề hiện tại | Hướng xử lý | Khi nào xem là xong |
|---|---|---|---|
| 1 | Chưa diễn tập khôi phục từ backup | Backup warehouse + Airflow metadata và key/config cần thiết; restore vào volume/database **riêng** | Restore được, đối chiếu row counts và lịch sử DAG; không đụng DB đang dùng |
| 2 | Event quá cũ có thể nằm ngoài lookback raw/Silver | Bounded backfill `[start,end)`, ghi row count/watermark trước–sau và rebuild nhánh Silver cần thiết | Fixture có late event ngoài 2 giờ được sửa đúng từ raw đến Gold |
| 3 | Gold build từng model, lỗi giữa chừng có thể để dashboard thấy các phiên bản khác nhau | Nghiên cứu build staging schema rồi publish nhất quán, hoặc khóa/đánh dấu phiên bản được phục vụ | Test gây lỗi giữa build chứng minh dashboard vẫn thấy bản Gold đã kiểm chứng trước đó |
| 4 | Snapshot mới kiểm tập tên cột; event lỗi chưa có nơi cách ly | Contract cột/type/JSON, quarantine có lý do và test schema drift | Batch lỗi được báo rõ; dữ liệu tốt không bị nhầm với dữ liệu bị bỏ |
| 5 | Extract tạo DataFrame toàn bộ trong RAM | Đo RAM/time theo kích thước nguồn; chọn pagination/streaming và batch insert nếu cần | Chạy volume mục tiêu dưới trần RAM, có số đo trước/sau |
| Theo nhu cầu | Airflow chỉ có một task `build` cho toàn dbt | [Cosmos plan](cosmos.md): tách task theo model, giữ guard/limits/CI | DAG graph, retries, artifacts và đối soát được kiểm chứng trên stack mới |
| Theo nhu cầu | Image tags, logs/artifacts và cài máy mới chưa có quy trình dài hạn hoàn chỉnh | Pin digest khi release, retention có backup, clean-machine smoke và restore drill | Người khác clone/build/run được theo README; báo cáo nêu rõ môi trường và kết quả |

Không cần triển khai hết để gọi project hiện tại là hoàn thiện cho mục tiêu học tập. Trước khi làm một hạng mục, đọc [kiến trúc và giới hạn hiện tại](../architecture/data_flow.md), viết fixture/lỗi mô phỏng, rồi cập nhật đúng guide vận hành liên quan. Kế hoạch Cosmos là một nhánh **tùy chọn**, không phải bước bắt buộc sau Docker.
