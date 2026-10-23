# Quản lý RAM/CPU cho Airflow platform dùng chung

**Dùng khi:** muốn biết trần RAM/CPU, bật thêm service hoặc đưa project khác vào Airflow dùng chung. Người chỉ chạy LapLapTech với cấu hình mặc định có thể bỏ qua tài liệu này.

## 1. Ba lớp quản lý

Một Airflow platform phục vụ nhiều project. Mỗi project có DAG/runtime/config riêng, cùng dùng ngân sách tài nguyên khai báo trong [`resources.json`](../../infra/airflow-platform/resources.json). Không cần dựng thêm scheduler/API/metadata cho mỗi project. Nếu mới học cách chia sẻ platform, đọc [sơ đồ và khái niệm chung](../architecture/shared_platform.md) trước khi sửa policy.

| Lớp | Cơ chế | Tác dụng |
|---|---|---|
| Windows/WSL | `.wslconfig`: 8 GiB RAM, 6 CPU logic, 2 GiB swap | Trần của toàn bộ WSL2, gồm Docker và distro khác |
| Docker | Memory limit, CPU quota, PID limit, không swap cho từng container | Một container không được vượt trần đã khai báo |
| Airflow | Pool `platform_memory`: 4 slot × 512 MiB; LocalExecutor parallelism 2 | Chỉ nhận task khi đủ slot; tối đa hai Airflow task cùng chạy |

Pool là phép tính ngân sách, không cấp phát RAM thật. Docker mới thực thi trần bộ nhớ/CPU. Limits là mức tối đa, không phải RAM được giữ sẵn. Task/CLI không dùng operator chung sẽ không được pool này quản lý.

Baseline được chọn cho máy khoảng 16 GB RAM/18 CPU logic; giữ phần còn lại cho Windows, VS Code và trình duyệt. `memory=8192MB` trong WSL là 8192 MiB; Docker Engine có thể báo ít hơn do VM/kernel. CPU limit của nhiều container có thể cộng vượt số CPU host: chúng chia thời gian CPU, không được bảo đảm chạy đồng thời ở mức tối đa.

## 2. Ngân sách hiện tại

| Service thường trực | RAM tối đa | CPU tối đa |
|---|---:|---:|
| Metadata PostgreSQL | 512 MiB | 0,5 |
| Airflow API | 1024 MiB | 1 |
| Airflow scheduler + LocalExecutor | 1024 MiB | 1,5 |
| DAG processor | 512 MiB | 1 |
| Warehouse PostgreSQL | 1024 MiB | 1,5 |
| Dashboard, bật khi cần | 512 MiB | 0,5 |
| **Tổng mặc định, không gồm dashboard** | **4096 MiB** | |

`airflow-init` dùng tối đa 1024 MiB/1 CPU khi khởi tạo; không tính là service thường trực. Các Airflow service chờ init hoàn tất khi khởi động.

| Task LaplapTech | RAM tối đa | CPU tối đa | Slot |
|---|---:|---:|---:|
| ingest | 2048 MiB | 2 | 4 |
| freshness | 512 MiB | 1 | 1 |
| build | 1024 MiB | 1 | 2 |
| reconcile | 512 MiB | 1 | 1 |

Slot của task = `ceil(memory_mib / 512)`. Ingest chiếm cả pool; build có thể chia pool với task nhẹ của project khác. LaplapTech vẫn giữ `max_active_runs=1`, `max_active_tasks=1` để tránh writer chồng nhau. Giới hạn này không bảo vệ writer chạy thủ công ngoài Airflow.

Ngân sách host mặc định: **4096 + 2048 + 768 headroom + 512 VM overhead = 7424 MiB ≤ 8192 MiB**. Dashboard dùng Compose profile `dashboard`; khi bật, ngân sách thành 7936 MiB. Service của project khác trên cùng máy phải được cộng vào ngân sách platform trước khi chạy cùng; mẫu repo không tự đăng ký chúng. Không dùng swap để mở rộng ngân sách task.

Service thuộc project khác được đăng ký trong `external_services` của `airflow-platform/resources.json` với `memory_mib`, `cpus`, `compose_project`, `compose_service` và `optional: true` nếu chỉ bật khi cần. Audit tìm đúng Compose labels rồi kiểm Docker memory/CPU/PID/health. Service tùy chọn chỉ cộng vào tổng khi đang chạy; service thường trực luôn được tính. Một lệnh `docker compose up` thủ công có thể bật hai service đồng thời, vì vậy chạy `python check_resources.py` sau khi bật/tắt và xử lý lỗi ngân sách trước khi chạy task nặng.

dbt mặc định hai thread; BLAS/OMP/MKL một thread. PostgreSQL warehouse đặt `shared_buffers=256MB`, `work_mem=8MB`, `max_connections=40`, tắt parallel worker mỗi gather. `work_mem` tính theo thao tác SQL/connection, không phải tổng bộ nhớ PostgreSQL. Metadata dùng cấu hình nhỏ hơn. Docker log xoay vòng 10 MB × 3; Airflow logs/artifact volumes vẫn cần backup và dọn theo nhu cầu.

## 3. File nào là nguồn cấu hình?

- Trong repo: policy mặc định, operator dùng chung và Compose dành người clone.
- Trong workspace **`airflow-platform/resources.json`**: policy vận hành, gồm đăng ký project của bạn. Bootstrap giữ file này nếu đã có; thay mặc định trong repo không tự ghi đè cấu hình đang dùng.
- `Configure-Resources.ps1` kiểm tra ngân sách mặc định rồi sinh biến `RESOURCE_*` trong `.env` platform. Nó giữ password/keys và backup `.env` trước khi sửa; không tự recreate container. `check_resources.py` cộng thêm các service tùy chọn thực sự đang chạy và báo lỗi nếu tổ hợp vượt ngân sách.
- [`BudgetDockerOperator`](../../infra/airflow-platform/dags/budget_docker.py) đọc policy được mount read-only, gán pool/slot và Docker limits. Với Docker provider 4.6.1, `cpus` của DockerOperator là CPU shares; wrapper bổ sung `NanoCpus` để có trần CPU cứng. Khi nâng provider phải kiểm thử lại wrapper.

Để kiểm tra policy tại **workspace platform**:

```powershell
.\Configure-Resources.ps1 -CheckOnly
```

Để thay policy đang hoạt động: ghi lại DAG nào đang unpaused, pause chúng và đợi toàn platform hết queued/running runs cùng task containers; sau đó mới sửa `resources.json`. Chạy lại bootstrap từ **root repo** để sinh `.env`, nạp lại `DBT_THREADS` vào config volume, recreate services và đồng bộ pool:

```powershell
.\scripts\Start-LocalStack.ps1
```

Nếu bạn đã pause thủ công, bootstrap giữ nguyên trạng thái đó; kiểm tra thành công rồi unpause đúng các DAG đã ghi lại. Nếu bootstrap tự pause, nó khôi phục các DAG trước đó unpaused khi thành công; lỗi giữa cập nhật sẽ giữ paused. Danh sách đã pause được lưu trong `backups/bootstrap-*/previously-unpaused-dags.json` để phục hồi sau lỗi. Không chỉnh pool lớn hơn policy trong UI để giải quyết task queued.

## 4. Giới hạn Windows/WSL

Tại **workspace platform**:

```powershell
.\Configure-Resources.ps1 -ApplyWslConfig
```

Lệnh merge `%USERPROFILE%\.wslconfig`, backup file cũ nếu có, đặt `[wsl2] memory=8192MB`, `processors=6`, `swap=2048MB` và `[experimental] autoMemoryReclaim=gradual`. Nó không restart WSL. Kiểm tra phần cứng sẽ từ chối cấu hình không để lại tối thiểu 6 GiB cho Windows hoặc yêu cầu nhiều CPU hơn máy có.

Thay đổi này chỉ có hiệu lực sau khi toàn bộ WSL dừng và khởi động lại. Đợi mọi project chạy xong, stop các stack cần giữ dữ liệu, thoát Docker Desktop; khi các distro đã dừng thì mở Docker lại và start stack. `wsl --shutdown` buộc dừng mọi distro, nên chỉ dùng khi đã chủ động dừng công việc liên quan. Không chạy nó trong khi pipeline/database khác đang làm việc.

Kiểm tra sau khi Docker khởi động lại:

```powershell
docker info --format '{{.NCPU}} CPU; {{.MemTotal}} bytes RAM'
python check_resources.py --output backups/resource-check.json
```

Audit dùng Python 3.10+ và Docker CLI, không cần package Python. Bootstrap/pipeline không yêu cầu Python trên host. File audit ghi giới hạn thực tế, health/OOM/restart, pool và usage; không in container environment chứa mật khẩu. CPU Engine chỉ giảm về 6 sau khi WSL áp dụng cấu hình mới; audit chưa coi trạng thái restart-pending là lỗi container.

## 5. Thêm project tiếp theo

### 5.1. Địa chỉ và quy ước dùng chung

Đây là hợp đồng tích hợp để người hoặc AI trong repo mới đọc trước khi setup. Chỉ dùng chung **warehouse PostgreSQL**, không đưa dữ liệu project vào PostgreSQL metadata Airflow.

| Thành phần | Địa chỉ/quy ước |
|---|---|
| Workspace chung mặc định trên máy này | `D:\Dev\Project\airflow-platform` |
| Airflow UI | `http://localhost:8080`; đọc cổng thực tế trong `.env` platform nếu đã đổi |
| PostgreSQL từ máy host | `127.0.0.1:55434`; thực tế theo `WAREHOUSE_HOST_PORT` |
| PostgreSQL từ runtime container | `warehouse-laplaptech:5432`, container phải tham gia network `laplaptech-lab` |
| Database/login của project mới | Tên riêng, ví dụ database `next_project_dev`, login `next_project` |
| Resource registry | `resources.json` tại workspace chung; không tạo pool RAM riêng để vượt ngân sách |
| Secrets | `.env`/config volume riêng của từng project, không commit hoặc ghi vào AGENTS |

Tên service `warehouse-laplaptech` được giữ để tương thích cấu hình và guards hiện có; PostgreSQL này có thể chứa thêm database. Máy clone khác có workspace/port riêng, không tự kết nối vào máy người duy trì repo. Airflow vẫn dùng chung UI/metadata nên mô hình này dành cho các project local tin cậy.

### 5.2. Tạo database và user riêng khi có project mới

Chỉ thực hiện khi đã chọn tên project thật; đoạn dưới là mẫu, chưa tạo thêm database/user. Mở psql bằng admin hiện có:

```powershell
docker exec -it airflow-platform-warehouse-laplaptech-1 psql -U laplaptech -d postgres -v ON_ERROR_STOP=1
```

Trong **psql**, thay tên mẫu trước khi chạy. `\password` hỏi mật khẩu tương tác; không ghi password vào SQL/history:

```sql
CREATE ROLE next_project LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 8;
\password next_project
CREATE DATABASE next_project_dev OWNER next_project TEMPLATE template0;
REVOKE ALL ON DATABASE next_project_dev FROM PUBLIC;
GRANT CONNECT, TEMPORARY ON DATABASE next_project_dev TO next_project;
\connect next_project_dev
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE, CREATE ON SCHEMA public TO next_project;
\quit
```

`CREATE DATABASE` phải chạy ngoài transaction; không gộp đoạn này vào một `psql -c` nhiều SQL hoặc `BEGIN`. Lệnh cố ý báo lỗi khi tên đã tồn tại để kiểm tra, không tự reset database. Kiểm tra lại bằng login mới qua TCP để xác nhận cả mật khẩu lẫn identity:

```powershell
docker exec -it airflow-platform-warehouse-laplaptech-1 psql -h 127.0.0.1 -U next_project -d next_project_dev -W -c 'SELECT current_database(), current_user;'
```

Đưa login riêng vào config project mới. Từ host dùng port publish 55434; từ runtime dùng DNS Docker/5432 như bảng trên. Không dùng `localhost` để runtime container tìm PostgreSQL ở container khác. Mỗi project có tối đa tám kết nối theo mẫu; warehouse hiện có tổng `max_connections=40`, nên không nhân thêm dashboard/kết nối không kiểm soát.

User `laplaptech` hiện là admin khởi tạo PostgreSQL và runtime LaplapTech đang dùng user đó; không chia sẻ login này cho project mới. Database/user riêng giúp phân tách đối tượng và quyền, không tạo tenant isolation tuyệt đối: superuser vẫn có quyền toàn cluster, tài nguyên CPU/RAM cùng được chia sẻ. Database tạo bằng mẫu đã thu hồi quyền PUBLIC; các database cũ cần rà quyền riêng khi thực sự thêm project, không suy ra chúng đã được khóa chỉ vì tạo login mới. [PostgreSQL roles](https://www.postgresql.org/docs/16/sql-createrole.html), [privileges](https://www.postgresql.org/docs/16/ddl-priv.html), [psql](https://www.postgresql.org/docs/16/app-psql.html).

### 5.3. Để project/AI tiếp theo biết cấu hình này

Bootstrap tạo **`airflow-platform/AGENTS.md`** và giữ nguyên nếu file đã có. File này trỏ đến guide hiện tại, resource registry và quy ước database/user/network. Nó chỉ tự áp dụng trong workspace đó; một repo khác không tự đọc file ở thư mục sibling.

Khi bắt đầu project mới, đưa đoạn tham chiếu sau vào **AGENTS.md của chính repo mới**, hoặc gửi nguyên nội dung này trong chat setup. Đổi đường dẫn nếu clone ở vị trí khác:

```text
Project này dùng shared local data platform.
Trước khi thiết lập Docker/Airflow/PostgreSQL, hãy đọc:
- D:/Dev/Project/airflow-platform/AGENTS.md
- D:/Dev/Project/laplaptech_pipeline/docs/guides/resources.md
- D:/Dev/Project/airflow-platform/resources.json
Dùng lại Airflow và warehouse PostgreSQL đang có; tạo database/user riêng cho project.
Giữ nguyên đăng ký, dữ liệu và keys của các project khác.
Đăng ký RAM/CPU task và dùng BudgetDockerOperator/pool chung trước khi chạy.
Không lấy credential admin hoặc config volume LaplapTech làm cấu hình project này.
```

Runtime/entrypoint của project mới phải kiểm tra đúng database/user của chính nó. `scripts/run_airflow_stage.py` của LaplapTech cố ý khóa target LaplapTech; không copy nguyên wrapper này rồi bỏ guard để chạy database khác. Hãy viết wrapper/config riêng và giữ nguyên tắc kiểm tra target trước khi ghi.

### 5.4. Đăng ký task và triển khai

1. Dùng lại workspace/control plane; tạo image, DAG ID và config/artifacts riêng cho project.
2. Khi platform đã drain, thêm entry vào `projects` của **policy vận hành**. Ví dụ project dùng một database/user riêng trên PostgreSQL đã có, không tạo thêm service thường trực:

```json
"next_project": {
  "dbt_threads": 2,
  "services": {},
  "tasks": {
    "transform": {"memory_mib": 1024, "cpus": 1}
  }
}
```

Nếu tạo database service/dashboard mới, phải khai báo cả ngân sách idle trong `services`, ví dụ `"warehouse": {"memory_mib":512,"cpus":0.5,"compose_service":"warehouse-next"}`; thêm service Compose có `mem_limit`, `memswap_limit`, `cpus`, PID/log guardrails và biến `RESOURCE_NEXT_PROJECT_WAREHOUSE_*` tương ứng. Không tự động sinh database/service từ JSON. Budget vượt trần sẽ bị từ chối: giảm service idle/pool hoặc nâng cấu hình host có chủ đích.

3. Mount DAG project vào cùng Airflow như LaplapTech; thêm network/volume/config cần thiết. Dùng operator chung cho từng task Docker:

```python
from budget_docker import BudgetDockerOperator

# Bên trong DAG của next_project; runtime image/command do project đó đóng gói.
transform = BudgetDockerOperator(
    task_id="transform", resource_project="next_project", resource_stage="transform",
    image="next-project-runtime:local-v1", command=["transform"],
    docker_url="unix://var/run/docker.sock", api_version="auto",
    mount_tmp_dir=False, network_mode="laplaptech-lab",
    auto_remove="force", do_xcom_push=False,
    log_opts_max_size="10m", log_opts_max_file="3",
)
```

Đây là mẫu tích hợp, chưa chạy được trước khi bạn build image và cấu hình DB/config mounts. Không truyền lại `pool`, `pool_slots`, `mem_limit`, `cpus`: operator lấy chúng từ policy; task chưa đăng ký sẽ fail parse thay vì âm thầm chạy không giới hạn. Đặt timeout, retry và concurrency phù hợp trong DAG riêng.

4. Chạy `-CheckOnly`, cập nhật Compose khi đã drain, đồng bộ pool bằng bootstrap và kiểm tra import errors/audit. Bootstrap hiện quản lý hai Compose file của LaplapTech; overlay project mới phải được đưa vào lệnh triển khai riêng. Nó giữ đăng ký JSON của project khác nhưng không tự deploy overlay/image của chúng.

## 6. Theo dõi và xử lý

```powershell
docker stats --no-stream
python check_resources.py --output backups/resource-check.json
docker inspect --format '{{.State.OOMKilled}} {{.RestartCount}}' airflow-platform-airflow-scheduler-1
```

Task report ghi `memory.max`, `cpu.max`, `pids.max` từ cgroup để đối chiếu giới hạn của đúng container. `cpu.max` gồm quota/period; ví dụ `100000 100000` tương ứng một CPU. Exit 137 cần đối chiếu OOM/log vì SIGKILL cũng có thể do tác động khác. Task tự xóa sau chạy nên dùng Airflow log/report; nếu OOM giết cả runtime trước khi ghi report thì có thể không có artifact.

Khi task queued, xem slot còn trống và run đang active; task 4 slot phải chờ cả pool rảnh. Khi OOM, xem kích thước batch/concurrency/SQL trước; nếu tăng RAM task phải tăng trọng số slot và kiểm tra lại toàn ngân sách. Không bỏ memory limit để làm run xanh.

Service Docker ngoài policy, lệnh host CLI và task dùng default pool không nằm trong phép tính này. Ví dụ database của project khác đang chạy độc lập vẫn dùng RAM chung của Docker; phải dành headroom hoặc đưa vào quản lý khi cần chạy cùng lúc. Headroom không bảo đảm chịu được mọi workload ngoài platform. Với task Python nặng, ưu tiên đưa vào runtime container dùng operator chung; smoke TaskFlow nhỏ chạy trong ngân sách scheduler.

Tài liệu gốc: [Docker resource constraints](https://docs.docker.com/engine/containers/resource_constraints/), [Compose services](https://docs.docker.com/reference/compose-file/services/), [WSL configuration](https://learn.microsoft.com/en-us/windows/wsl/wsl-config), [DockerOperator API](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html).
