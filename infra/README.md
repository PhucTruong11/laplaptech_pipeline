# Hạ tầng Docker

`airflow-platform/` là template **chỉ cài lần đầu vào thư mục rỗng** bằng `-InitializePlatform`. Workspace platform đang có là nguồn cấu hình dùng chung; bootstrap không copy đè từ template. Cài đặt/start/stop theo [Docker runbook](../docs/guides/docker.md); học DAG theo [Airflow](../docs/guides/airflow.md); hiểu sharing ở [kiến trúc dùng chung](../docs/architecture/shared_platform.md).

## 1. Các file cần đọc

| Source | Vai trò |
|---|---|
| [`airflow-platform/compose.yaml`](airflow-platform/compose.yaml) | Control plane: metadata, init, API, scheduler và DAG processor |
| [`airflow-platform/compose.laplaptech.yaml`](airflow-platform/compose.laplaptech.yaml) | Provider image, socket/source mount, warehouse PostgreSQL và Streamlit |
| [`airflow-platform/Dockerfile`](airflow-platform/Dockerfile) | Airflow 3.3.2/Python 3.13 + Docker provider 4.6.1, executable PATH cho user airflow |
| [`airflow-platform/Initialize-LocalEnv.ps1`](airflow-platform/Initialize-LocalEnv.ps1) | Sinh `.env` Airflow một lần; từ chối ghi đè keys hiện có |
| [`laplaptech/runtime.Dockerfile`](laplaptech/runtime.Dockerfile) | Python 3.13 + dependency lock + ingestion/dbt/stage entrypoint, UID 10001 |
| [`laplaptech/dashboard.Dockerfile`](laplaptech/dashboard.Dockerfile) | Mở rộng runtime với Streamlit app và HTTP server port 8501 |
| [`laplaptech/dags/laplaptech_pipeline.py`](laplaptech/dags/laplaptech_pipeline.py) | Bốn DockerOperator task: ingest → freshness → build → reconcile |
| [`airflow-platform/diagnostics/platform_smoke_test.py`](airflow-platform/diagnostics/platform_smoke_test.py) | Diagnostic tùy chọn khi nâng cấp Airflow; không deploy trong thư mục DAG mặc định |
| [`../scripts/Start-LocalStack.ps1`](../scripts/Start-LocalStack.ps1) | Build/config/bootstrap, tạo volumes/pool và bảo vệ lịch trong lúc cập nhật |
| [`../scripts/verify_local_stack.py`](../scripts/verify_local_stack.py) | Kiểm tra services, DAG/data/artifacts, dependency lock và dashboard |

Cả hai Compose file phải được dùng cùng nhau. Base riêng không chứa đầy đủ provider, warehouse và dashboard của LaplapTech. Project name mặc định là `airflow-platform`.

## 2. Images và service

| Image/service | Chức năng |
|---|---|
| `airflow-platform:3.3.2-docker4.6.1-py3.13` | API/scheduler/DAG processor/init dùng cùng core/provider |
| `laplaptech-runtime:local-v1` | Container task riêng được scheduler tạo qua Docker Engine |
| `laplaptech-dashboard:local-v1` | Dashboard bật khi cần qua Compose profile, đọc đủ 12 Gold relations |
| `postgres:16` / `metadata` | Database `airflow_meta` giữ state/config Airflow |
| `postgres:16` / `warehouse-laplaptech` | Database `laplaptech_pipeline_dev` giữ raw/Bronze/Silver/Gold |

Có năm service nền; dashboard bật riêng qua Compose profile `dashboard`. `airflow-init` thoát code 0 sau khởi tạo là bình thường. Task container chỉ tồn tại khi chạy task, tự xóa sau kết thúc. Airflow dùng LocalExecutor, không có worker/Celery/Redis riêng trong cấu hình này.

## 3. Cấu hình và source mount

| Phạm vi | Đọc ở đâu? |
|---|---|
| Root repo `.env` | Login ClickHouse nguồn; PostgreSQL host config cho lệnh từ máy |
| Workspace platform `.env` | Airflow/metadata keys, port host và `LAPLAPTECH_PROJECT_DIR` |
| Config volume | File `/run/laplaptech/pipeline.env` dành runtime, target PostgreSQL Docker cố định |

Bootstrap chỉ nhập nhóm `CLICKHOUSE_*`/`INGESTION_*` từ file nguồn; không sao chép endpoint PostgreSQL cloud vào task. Source mặc định là root `.env`, có thể chọn bằng `-SourceEnvFile`. `airflow-platform/.env.example` chỉ mô tả biến platform, không phải password để đăng nhập.

`LAPLAPTECH_PROJECT_DIR` trỏ repo; Compose bind mount `infra/laplaptech/dags` read-only vào `/opt/airflow/dags/laplaptech` của API/scheduler/DAG processor. Sửa DAG trong repo rồi đợi parse, không copy file Python trùng ID sang platform.

Sửa ingestion/dbt/stage entrypoint cần rebuild runtime; sửa app cần rebuild dashboard. Bootstrap tạm pause mọi DAG đang bật và kiểm tra active runs toàn platform khi cập nhật, chặn active run/orphan task và giữ keys. Lỗi sau khi bắt đầu cập nhật sẽ giữ DAG paused để xử lý; xem [Docker](../docs/guides/docker.md) trước khi thao tác.

## 4. Network, cổng và dữ liệu bền vững

- Control plane dùng network mặc định của Compose; metadata không publish cổng host.
- Task runtime, warehouse và dashboard dùng network `laplaptech-lab`.
- API publish `127.0.0.1:8080`, dashboard `127.0.0.1:8501`, warehouse `127.0.0.1:55434` theo mặc định.
- Trong task/dashboard, PostgreSQL là `warehouse-laplaptech:5432`; `localhost` bên trong container trỏ chính container đó.

| Named volume | Dữ liệu |
|---|---|
| `airflow-platform_airflow-metadata` | Metadata PostgreSQL Airflow |
| `airflow-platform_airflow-logs` | Task logs Airflow |
| `laplaptech-lab-warehouse` | Warehouse PostgreSQL |
| `laplaptech-lab-config` | Runtime env, owner UID 10001/mode 0400, mount read-only |
| `laplaptech-lab-artifacts` | Stage reports/dbt artifacts theo run/task/attempt, runtime ghi tại `/artifacts` |

Scheduler được cấp Docker socket để tạo runtime trên máy local tin cậy. API/DAG processor không nhận socket. Config volume chỉ đọc và dataset không qua XCom; dashboard không được cấp source ClickHouse credentials.

`stop`/recreate không tự xóa named volumes. Giữ `.env`/Fernet key cùng metadata backup; không dùng xóa volume như cách xử lý lỗi đăng nhập. Các project khác có thể dùng chung platform nhưng phải tách DAG ID/runtime/network/config, dùng chung pool ngân sách RAM; shared UI/metadata không phải tenant isolation.

## 5. Giới hạn và kiểm chứng

Baseline Airflow/provider được pin; base Python/PostgreSQL vẫn là tags, chưa pin image digest. Bootstrap đã kiểm chứng trên Windows Docker Desktop/workspace hiện có; fresh-machine, native Linux/socket GID, nâng major và restore drill có phạm vi kiểm tra riêng.

[Testing](../docs/guides/testing.md) giải thích cách kiểm tra lại và liệt kê bằng chứng theo ngày. Docker tắt thì local ETL/dashboard dừng; GitHub CI không thay scheduler.

## 6. Tài nguyên dùng chung

`airflow-platform/resources.json` khai báo ngân sách host, service và task cho nhiều project. `Configure-Resources.ps1` kiểm tra ngân sách/sinh biến Compose; `check_resources.py` audit giới hạn thực tế. `dags/budget_docker.py` và `platform_resource_policy.py` là operator/policy helper dùng chung. Bootstrap giữ policy vận hành đã có, mount read-only vào Airflow và đồng bộ pool `platform_memory`. Xem [runbook tài nguyên](../docs/guides/resources.md) cho cấu hình WSL, thêm project và giới hạn của phép tính ngân sách.

Workspace vận hành có `AGENTS.md` hướng dẫn tích hợp. Repo mới cần AGENTS/README của chính nó trỏ đến file đó và [hợp đồng tích hợp](../docs/guides/resources.md#5-thêm-project-tiếp-theo); agent trong repo khác không tự biết cấu hình sibling. PostgreSQL warehouse dùng chung server, mỗi project thêm database/login riêng; metadata Airflow vẫn tách biệt.
