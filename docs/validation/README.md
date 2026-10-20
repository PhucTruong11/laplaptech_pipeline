# Bằng chứng kiểm chứng

Mỗi report ghi thời điểm, phạm vi và kết quả của một lượt kiểm tra. Đây là snapshot, không phải health endpoint realtime. Hướng dẫn chạy lại nằm trong [testing](../guides/testing.md); cấu hình và luồng hệ thống xem [data flow](../architecture/data_flow.md).

| Report | Phạm vi đã ghi nhận |
|---|---|
| [System check 20/10/2026](system_check_2026-10-20.json) | Run Airflow mới; 784.914 event; 31 model/47 dbt data tests; đối soát và AppTest dữ liệu thật |
| [Regression 20/10/2026](regression_check_2026-10-20.json) | Fixture full/replay và 43/43 Python tests trên PostgreSQL test riêng |
| [Cutover 20/10/2026](local_cutover_2026-10-20.json) | Chuyển sang full Docker local, full-refresh/replay và kiểm tra việc gỡ workflow ETL cloud tại thời điểm đó |
| [Docker trước DAG thật](airflow_docker_setup_2026-10-20.json) | Lịch sử dựng control plane/runtime/volumes, smoke DAG và preflight |
| [Dev 19/10/2026](dev_run_2026-10-19.json) | Lịch sử nâng Gold/metrics/dashboard trên fixture và ClickHouse dev-live |

Từ root repo, kiểm tra hệ thống hiện có mà không trigger ETL:

```powershell
python scripts/verify_local_stack.py --output scratch/system-check.json
```

Thêm `--run-pipeline` khi muốn một run Airflow mới đọc nguồn/ghi warehouse. Fixture/regression chỉ dùng database test riêng theo testing guide, không ghi warehouse active.

Report không chứa credential. Logs/artifacts chi tiết nằm trong volumes hoặc `scratch/` ignored. Giữ JSON lịch sử nguyên trạng; tạo report mới thay vì đổi ý nghĩa kết quả cũ. Archive đọc được không đồng nghĩa đã diễn tập restore, và kiểm chứng local không xác nhận project Neon đã bị xóa.
