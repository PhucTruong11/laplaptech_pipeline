# Hạ tầng triển khai

Thư mục `infra/` lưu cấu hình hạ tầng của dự án. Logic ingestion, dbt và dashboard tiếp tục nằm trong các thư mục ứng dụng tương ứng.

## Airflow platform dùng chung

[`airflow-platform/`](airflow-platform) chứa cấu hình Docker Compose, script khởi tạo env và DAG kiểm tra nền tảng. Làm theo [hướng dẫn triển khai Airflow](../docs/guides/airflow_setup.md).

- **Cấu hình được quản lý trong repo:** `infra/airflow-platform/`.
- **Workspace vận hành trên máy cá nhân:** `D:\Dev\Project\airflow-platform`, được copy từ cấu hình trong repo. Các lệnh Compose thường ngày chạy tại workspace này.
- **DAG kiểm tra:** `platform_smoke_test`, dùng xác nhận task/log/XCom hoạt động; chưa chạy ingestion/dbt của LaplapTech.
- **DAG pipeline sắp tích hợp:** `laplaptech_lab`, dùng runtime và warehouse dev riêng; chưa được triển khai.

Tự viết phần tích hợp theo [code mẫu và giải thích DAG LaplapTech](../docs/guides/airflow_laplaptech_walkthrough.md): runtime, warehouse, provider, config/artifacts, preflight và ba task thật. Các file trong bài chưa được sinh tự động hoặc bật vào platform.

Nếu bạn đã copy cấu hình ra workspace vận hành, việc đổi tên thư mục trong repo không yêu cầu tạo lại `.env`, xóa containers hay volumes. Các thay đổi cấu hình về sau cần được đồng bộ có chủ đích sang workspace đó.

Bộ này khởi tạo nền tảng điều phối dùng chung. Kế hoạch đóng gói toàn bộ pipeline cho người clone nằm trong [Docker distribution plan](../docs/guides/docker_distribution_plan.md).
