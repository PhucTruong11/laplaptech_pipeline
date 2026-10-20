# Tài liệu LaplapTech

Bắt đầu từ [quickstart trong README](../README.md), sau đó chọn đúng hướng dẫn cho công việc. Hệ thống hiện hành chạy Docker local: ClickHouse public → Airflow task containers → PostgreSQL → Streamlit.

## Chạy và phát triển

| Hướng dẫn | Nội dung chính |
|---|---|
| [Docker](guides/docker.md) | Điều kiện, bootstrap, start/stop, kết nối PostgreSQL, rebuild, backup và xử lý lỗi |
| [Airflow](guides/airflow.md) | Control plane/runtime, DAG thật, lịch/param, logs, Assets, retry/Clear và smoke DAG |
| [dbt](guides/dbt.md) | Profile/env, run/test/build, source freshness, selectors, exposures và documentation |
| [Ingestion](guides/ingestion.md) | Target, transaction/version, manual ingestion và backfill có kiểm soát |
| [Testing](guides/testing.md) | Verifier full system, PostgreSQL fixture riêng, regression và CI |

Các runbook chỉ rõ **root repo** hay **workspace platform** trước khi đưa lệnh. Dùng source code được liên kết làm cấu hình chuẩn; không tạo bản DAG/Compose thứ hai từ một đoạn minh họa.

## Hiểu hệ thống và bằng chứng

| Tài liệu | Mục đích |
|---|---|
| [Data flow](architecture/data_flow.md) | Thành phần, lineage, grain/metric, failure path và giới hạn |
| [Hạ tầng](../infra/README.md) | Vai trò từng Compose/Dockerfile, mounts, networks và volumes |
| [Validation](validation/README.md) | Report theo thời điểm/phạm vi; phân biệt kiểm chứng mới với bằng chứng lịch sử |
| [Nguồn logo SVG](../assets/tool-logos.json) | Provenance và license của các logo trong sơ đồ |

Giữ report lịch sử ở JSON, không dùng nó như health endpoint realtime. Việc chuyển warehouse local không xác nhận project Neon đã được xóa. `.env`, source login, Airflow keys, logs và backup không được commit.
