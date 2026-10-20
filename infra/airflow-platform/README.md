# Nền tảng Airflow dùng chung

Cấu hình triển khai Airflow **3.3.2 / Python 3.13 / LocalExecutor** và PostgreSQL 16 metadata. Đây là nền tảng điều phối để tích hợp các dự án; hiện chưa chạy pipeline LaplapTech.

## Các file

| File | Vai trò |
|---|---|
| `compose.yaml` | Các service Airflow, metadata DB, network và volumes |
| `.env.example` | Danh sách biến cấu hình tham khảo; chưa có secret sử dụng được |
| `Initialize-LocalEnv.ps1` | Sinh `.env` local với mật khẩu và khóa ngẫu nhiên; không ghi đè file đã có |
| `dags/platform_smoke_test.py` | DAG kiểm tra hai task, XCom và log; không ghi dữ liệu pipeline |
| `.gitignore` | Giữ `.env`, cache và backup ngoài Git |

`smoke_test` có nghĩa là kiểm tra cơ bản nền tảng có hoạt động. Tên DAG được giữ ổn định để khớp workspace bạn đã copy và các lệnh trong hướng dẫn.

## Nơi chạy

Trong repo, bộ cấu hình nằm tại `infra/airflow-platform/`. Workspace vận hành dùng chung trên máy là `D:\Dev\Project\airflow-platform`.

Đọc [hướng dẫn triển khai từng bước](../../docs/guides/airflow_setup.md) tại repo trước khi copy sang workspace vận hành. Hướng dẫn không được copy cùng thư mục này. Nếu đã copy từ vị trí cũ `examples`, giữ nguyên workspace vận hành và `.env` hiện có.

UI mặc định: `http://localhost:8080`, chỉ bind `127.0.0.1`. Docker Desktop cần Linux containers; không cần cài Python/Airflow trên host. Mật khẩu và keys nằm trong `.env` do script tạo, cần giữ cùng backup metadata.

Compose này dùng LocalExecutor cho lab cá nhân; [quickstart chính thức](https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html) dùng CeleryExecutor. Chủ project đã gửi kết quả init/service health/smoke DAG success ngày 20/10/2026. Retry, restart và pipeline thật cần nghiệm thu riêng.

Sau phần khởi tạo, làm theo [bài tự viết DAG LaplapTech](../../docs/guides/airflow_laplaptech_walkthrough.md). Mẫu tích hợp nằm trong Markdown, chưa được kích hoạt trong Compose/DAG hiện tại.
