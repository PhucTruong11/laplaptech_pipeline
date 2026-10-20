# Tự viết DAG LaplapTech: code mẫu và giải thích từng bước

Cập nhật 20/10/2026. Bài tiếp theo sau [khởi tạo Airflow](airflow_setup.md). Bạn đã gửi ảnh `platform_smoke_test` chạy success với hai task. Bây giờ chuyển từ kiểm tra nền tảng sang điều phối pipeline thật trên **warehouse dev riêng**.

**Cách học:** tự tạo từng file theo các block có nhãn `sample-file`, đọc giải thích, rồi chạy checkpoint của bài đó. Các file triển khai trong bài **chưa được tạo hoặc bật tự động**; chúng nằm trong tài liệu để bạn code theo. Mẫu đã được kiểm tra cú pháp/cấu hình tĩnh, chưa được build image hoặc chạy end-to-end trên Docker. Không coi đọc hết tài liệu là đã nghiệm thu pipeline.

## 1. Airflow gọi gì, phần nào vẫn do project thực hiện?

```text
Airflow scheduler
  └─ DockerOperator yêu cầu Docker Engine tạo task container
       ├─ ingest:    Python ingestion hiện có → warehouse raw
       ├─ freshness: dbt source freshness → kiểm tra độ mới
       └─ build:     dbt build → Bronze / Silver / Gold + data tests
```

Ba task là ba container lần lượt dùng **cùng image runtime LaplapTech**. Dataset nằm trong PostgreSQL; không truyền DataFrame qua XCom. dbt tự xử lý dependency `ref()` giữa các model, Airflow quản lý dependency giữa các bước lớn.

| Thành phần | Đích / trách nhiệm |
|---|---|
| Airflow metadata hiện có | User, DAG/task/run, trạng thái điều phối |
| `warehouse-laplaptech` mới | Database `laplaptech_pipeline_dev`, lưu raw và các layer dbt |
| Network `laplaptech-lab` | Task container tìm warehouse bằng DNS service name |
| Volume `laplaptech-lab-config` | Một file cấu hình runtime, task chỉ đọc |
| Volume `laplaptech-lab-artifacts` | Kết quả dbt và stage report theo run/stage/attempt |
| Neon + GitHub Actions hiện tại | Tiếp tục chạy như trước, không dùng cấu hình lab này |

`localhost` bên trong task container là chính container đó. Vì vậy đích lab phải là **`warehouse-laplaptech:5432`**, không phải localhost và cũng không phải Neon.

## 2. Hai workspace: viết file ở đâu?

```text
D:\Dev\Project\laplaptech_pipeline\        # source project
  .dockerignore                           # bạn sẽ tạo
  scripts/run_airflow_stage.py             # entrypoint mới, bạn sẽ tạo
  infra/laplaptech/
    runtime.Dockerfile                    # bạn sẽ tạo
    dags/laplaptech_lab.py                 # bạn sẽ tạo, chưa auto-mount

D:\Dev\Project\airflow-platform\           # Airflow đang chạy
  compose.yaml                            # đã có, giữ tên project/volumes/keys
  .env                                    # đã có, giữ nguyên
  Dockerfile                              # image Airflow có Docker provider, sẽ tạo
  compose.laplaptech.yaml                  # cấu hình bổ sung, sẽ tạo
  config/.env.laplaptech                   # cấu hình lab, sẽ tạo
  dags/laplaptech/laplaptech_lab.py         # copy DAG sau khi provider/DB sẵn sàng
```

Mỗi block phía dưới ghi rõ workspace. `.env` platform là cấu hình Airflow; `config/.env.laplaptech` là cấu hình pipeline. Không copy root `.env` đang dùng Neon vào runtime image hoặc cấu hình lab.

Chỉ copy **nội dung bên trong** các khung code; ba dấu backtick là ký hiệu Markdown, không phải lệnh PowerShell.

## 3. Bài 1 — cấu hình nguồn public và đích lab

Trong workspace **airflow-platform**, tạo thư mục `config` và file dưới đây. Điền mật khẩu nguồn ClickHouse public đang dùng; placeholder trong mẫu chưa thể kết nối nguồn.

<!-- sample-file: platform/config/.env.laplaptech -->
```dotenv
CLICKHOUSE_HOST=applog.xomdata.com
CLICKHOUSE_PORT=80
CLICKHOUSE_SECURE=false
CLICKHOUSE_USER=xomdata
CLICKHOUSE_PASSWORD=REPLACE_WITH_PUBLIC_DATASET_PASSWORD
CLICKHOUSE_DATABASE=laplaptech

POSTGRES_HOST=warehouse-laplaptech
POSTGRES_PORT=5432
POSTGRES_USER=laplaptech
POSTGRES_PASSWORD=laplaptech_lab_only
POSTGRES_DATABASE=laplaptech_pipeline_dev
POSTGRES_SSLMODE=prefer
POSTGRES_CREATE_DATABASE=false
INGESTION_LOOKBACK_SECONDS=7200
```

`laplaptech_lab_only` là mật khẩu **DB local của bài thực hành**, khớp với Compose mục 6. Nếu muốn đổi thì sửa cả hai nơi trước khi init warehouse. Khi DB đã có volume, sửa env không tự đổi password trong PostgreSQL.

Trong `.gitignore` của **workspace airflow-platform**, đảm bảo có các dòng sau. `.env.*` bảo vệ cả `config/.env.laplaptech`; giữ thêm các rule cache/backup đã có.

```gitignore
.env
.env.*
!.env.example
```

**Tự giải thích:** nguồn có thể public nhưng đích phải riêng; không lấy password/endpoint Neon để điền vào DB lab. `POSTGRES_CREATE_DATABASE=false` vì PostgreSQL container sẽ cấp sẵn database.

## 4. Bài 2 — entrypoint chạy từng stage, có guard đích ghi

Trong **repo LaplapTech**, tạo `scripts/run_airflow_stage.py` theo mẫu. Script này là file mới, không sửa guard localhost của `scripts/run_dev_pipeline.py`.

<!-- sample-file: repo/scripts/run_airflow_stage.py -->
```python
"""Run one pipeline stage against the explicitly configured Docker lab only."""

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

from dotenv import dotenv_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


def lab_environment() -> dict[str, str]:
    config_path = Path('/run/laplaptech/pipeline.env')
    if not config_path.is_file():
        raise RuntimeError('Missing config volume: /run/laplaptech/pipeline.env')
    config = dotenv_values(config_path, interpolate=False)
    expected = {
        'POSTGRES_HOST': 'warehouse-laplaptech',
        'POSTGRES_PORT': '5432',
        'POSTGRES_USER': 'laplaptech',
        'POSTGRES_DATABASE': 'laplaptech_pipeline_dev',
        'POSTGRES_SSLMODE': 'prefer',
        'POSTGRES_CREATE_DATABASE': 'false',
    }
    for key, wanted in expected.items():
        if config.get(key) != wanted:
            raise RuntimeError(f'Lab guard: {key} must equal {wanted}')
    if not config.get('POSTGRES_PASSWORD'):
        raise RuntimeError('Missing lab database password')

    env = os.environ.copy()
    for key, value in config.items():
        if key.startswith(('POSTGRES_', 'CLICKHOUSE_', 'INGESTION_')) and value is not None:
            env[key] = value
    env['DBT_SEND_ANONYMOUS_USAGE_STATS'] = 'false'
    return env


def verify_database(env: dict[str, str]) -> None:
    url = URL.create(
        'postgresql+psycopg2',
        username=env['POSTGRES_USER'], password=env['POSTGRES_PASSWORD'],
        host=env['POSTGRES_HOST'], port=int(env['POSTGRES_PORT']),
        database=env['POSTGRES_DATABASE'],
        query={'sslmode': env['POSTGRES_SSLMODE']},
    )
    engine = create_engine(url, connect_args={'connect_timeout': 10})
    try:
        with engine.connect() as connection:
            database, username = connection.execute(
                text('SELECT current_database(), current_user')
            ).one()
        if (database, username) != ('laplaptech_pipeline_dev', 'laplaptech'):
            raise RuntimeError('Lab database identity did not match')
    finally:
        engine.dispose()


def stage_command(stage: str, output: Path) -> list[str]:
    if stage == 'ingest':
        return [sys.executable, '-m', 'ingestion.clickhouse_to_postgres']
    arguments = ['source', 'freshness'] if stage == 'freshness' else ['build']
    return ['dbt', *arguments,
            '--project-dir', '/app/dbt', '--profiles-dir', '/app/dbt',
            '--target-path', str(output / 'target'),
            '--log-path', str(output / 'logs')]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['preflight', 'ingest', 'freshness', 'build'])
    args = parser.parse_args()
    run_id = os.environ.get('PIPELINE_RUN_ID') or 'cli-' + uuid4().hex
    run_key = hashlib.sha256(run_id.encode()).hexdigest()[:20]
    attempt = os.environ.get('PIPELINE_ATTEMPT', '1')
    if not attempt.isdigit():
        raise RuntimeError('PIPELINE_ATTEMPT must be numeric')
    output = (Path('/artifacts') / run_key / args.stage /
              f'attempt-{attempt}-{uuid4().hex[:8]}')
    output.mkdir(parents=True, exist_ok=False)
    report = {
        'run_id': run_id, 'stage': args.stage, 'attempt': int(attempt),
        'started_at': datetime.now(timezone.utc).isoformat(),
        'status': 'failed', 'exit_code': 1,
    }
    try:
        env = lab_environment()
        verify_database(env)  # Read-only target check before every stage.
        print('Target verified: warehouse-laplaptech / laplaptech_pipeline_dev', flush=True)
        if args.stage == 'ingest':
            password = env.get('CLICKHOUSE_PASSWORD', '')
            if not password or password == 'REPLACE_WITH_PUBLIC_DATASET_PASSWORD':
                raise RuntimeError('Fill the public ClickHouse login before ingestion')
        code = 0 if args.stage == 'preflight' else subprocess.run(
            stage_command(args.stage, output), cwd='/app', env=env, check=False,
        ).returncode
        report.update(status='success' if code == 0 else 'failed', exit_code=code)
        return code
    finally:
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        (output / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(f'Stage artifacts: {output}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
```

### Vì sao viết như vậy?

- `lab_environment()` đọc file config ở volume, kiểm tra **host + port + user + database + policy**. Chỉ tên database có chữ “dev” chưa đủ cô lập đích. Đây là guard cho lab với network/config do bạn kiểm soát, không phải sandbox chống một Docker administrator độc hại.
- `interpolate=False` giữ ký tự `$` trong mật khẩu; không mở rộng biến ngoài ý muốn.
- `verify_database()` kiểm tra database/user thực tế bằng câu SELECT trước khi chạy stage; không tạo/xóa bảng ở checkpoint `preflight`.
- `stage_command()` gọi đúng CLI hiện có; không viết lại ETL hoặc SQL model trong DAG. Không có root `.env` trong image nên ingestion không tự đọc cấu hình Neon.
- `subprocess.run(..., env=env)` truyền cùng đích cho ingestion và dbt. Exit code khác 0 được trả về container để DockerOperator đánh dấu task fail.
- `run_id` được hash thành tên thư mục an toàn; mỗi stage/attempt có thư mục riêng và suffix để không ghi đè artifacts khi chạy lại. Report không ghi password hoặc toàn bộ env.
- `finally` giữ report kể cả khi guard/DB/stage lỗi; report file là bằng chứng thực hành, **chưa thay thế bảng run audit** hoặc heartbeat. Nếu process bị kill cứng thì report có thể chưa được ghi.

**Checkpoint đọc code:** chỉ ra vị trí chặn Neon, vị trí gọi ingestion hiện có và vị trí truyền lỗi dbt về Airflow. Đừng bỏ guard để cho lệnh “chạy được”.

## 5. Bài 3 — image runtime riêng cho pipeline

Trong **repo LaplapTech**, tạo root `.dockerignore`. Nếu file đã có, hợp nhất các rule thay vì ghi đè các rule riêng.

<!-- sample-file: repo/.dockerignore -->
```dockerignore
.git
.env
.env.*
**/.env
**/.env.*
venv
.venv
scratch
logs
**/__pycache__
dbt/target
dbt/logs
dbt/dbt_packages
dbt/.user.yml
.streamlit
streamlit_app/.streamlit
```

Tạo Dockerfile sau trong repo. Đây là image thực thi task, không phải đóng gói toàn bộ dashboard/Airflow cho bản public.

<!-- sample-file: repo/infra/laplaptech/runtime.Dockerfile -->
```dockerfile
FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
WORKDIR /app
RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin pipeline
COPY requirements.txt requirements.lock.txt ./
RUN pip install --no-cache-dir -r requirements.txt && pip check
COPY --chown=pipeline:pipeline ingestion ./ingestion
COPY --chown=pipeline:pipeline dbt ./dbt
COPY --chown=pipeline:pipeline scripts/run_airflow_stage.py ./scripts/run_airflow_stage.py
RUN mkdir -p /artifacts && chown -R pipeline:pipeline /app /artifacts
USER pipeline
ENTRYPOINT ["python", "-m", "scripts.run_airflow_stage"]
```

Giải thích: `FROM` chọn Python 3.13 khớp lock; COPY requirements trước source để tận dụng cache; chỉ copy source cần thiết; user `10001` chạy task và phải có quyền ghi artifacts. Tag Python slim theo minor, chưa khóa base digest. Provider Airflow không được cài vào image này. Repo hiện không có `packages.yml`, chưa cần `dbt deps`; nếu thêm package dbt về sau, cập nhật bước build tương ứng.

**Checkpoint build**, chạy tại root repo:

```powershell
Set-Location D:\Dev\Project\laplaptech_pipeline
docker build -f infra/laplaptech/runtime.Dockerfile -t laplaptech-runtime:lab-v1 .
docker run --rm --entrypoint pip laplaptech-runtime:lab-v1 check
```

Lệnh cuối cần trả về `No broken requirements found`. Chưa chạy ingestion ở bài này. Khi sửa source/lock/entrypoint, rebuild image và tăng tag trong DAG để biết run dùng bản nào; không giả định sửa file host tự cập nhật image đã build.

## 6. Bài 4 — bổ sung provider và warehouse vào platform

Trong **workspace airflow-platform**, tạo `Dockerfile`:

<!-- sample-file: platform/Dockerfile -->
```dockerfile
FROM apache/airflow:3.3.2-python3.13
USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" "apache-airflow-providers-docker==4.6.1" \
    && pip check
```

Provider Docker 4.6.1 công bố yêu cầu Airflow >=2.11.0 và Docker SDK >=7.1.0; mẫu chọn bản này để build/kiểm tra với Airflow 3.3.2. Đây chưa phải matrix đã nghiệm thu end-to-end. Pin cả Airflow khi cài provider để resolver không tự thay version core. Không dùng `_PIP_ADDITIONAL_REQUIREMENTS` để cài lại mỗi lần start. [Provider requirements](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/index.html), [hướng dẫn mở rộng image](https://airflow.apache.org/docs/docker-stack/build.html).

Image platform chỉ cần Dockerfile, nên thêm `.dockerignore` ở **workspace platform** để không gửi config/password vào build context:

<!-- sample-file: platform/.dockerignore -->
```dockerignore
**
!Dockerfile
```

Tạo file bổ sung sau, giữ `compose.yaml` và các keys `.env` đang dùng:

<!-- sample-file: platform/compose.laplaptech.yaml -->
```yaml
x-platform-image: &platform-image
  image: airflow-platform:3.3.2-docker4.6.1-py3.13
  build:
    context: .
    dockerfile: Dockerfile

services:
  airflow-init:
    <<: *platform-image
  airflow-api-server:
    <<: *platform-image
  airflow-dag-processor:
    <<: *platform-image
  airflow-scheduler:
    <<: *platform-image
    group_add:
      - "${DOCKER_SOCKET_GID:-0}"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock

  warehouse-laplaptech:
    image: postgres:16
    environment:
      POSTGRES_USER: laplaptech
      POSTGRES_PASSWORD: laplaptech_lab_only
      POSTGRES_DB: laplaptech_pipeline_dev
    volumes:
      - laplaptech-warehouse:/var/lib/postgresql/data
    networks:
      - laplaptech-lab
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U laplaptech -d laplaptech_pipeline_dev"]
      interval: 10s
      timeout: 5s
      retries: 10
      start_period: 10s
    restart: unless-stopped

networks:
  laplaptech-lab:
    name: laplaptech-lab
    driver: bridge

volumes:
  laplaptech-warehouse:
    name: laplaptech-lab-warehouse
```

Compose ghép file base trước, file bổ sung sau; image được thay, còn environment/healthcheck/metadata/DAG mount hiện có được giữ. Relative path tính từ file base, nên hai file ở cùng workspace. YAML anchor không dùng xuyên file: anchor ở đây chỉ chứa phần bổ sung. [Compose merge rules](https://docs.docker.com/compose/how-tos/multiple-compose-files/merge/).

Compose quản lý volume warehouse vì có service mount nó. Hai volume config/artifacts được cấp bằng Docker CLI ở mục 7, rồi DockerOperator gắn vào task container; không cần khai báo chúng như mount của scheduler hoặc API server.

DockerOperator tạo **container cùng Docker Engine**, không phải Docker Engine khác nằm trong scheduler. Socket cho scheduler quyền điều khiển Engine của máy; vì vậy chỉ cấp ở lab cá nhân này, không coi đây là cách cô lập người dùng/project không tin cậy nhau. API server và DAG processor không cần mount socket.

Kiểm tra group của socket bằng image đã tải, không cài Docker CLI trong scheduler:

```powershell
Set-Location D:\Dev\Project\airflow-platform
docker run --rm --entrypoint stat --mount type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock apache/airflow:3.3.2-python3.13 -c '%g' /var/run/docker.sock
```

Điền số in ra thành `DOCKER_SOCKET_GID=<số đó>` trong `.env` platform, thêm nếu chưa có. Không cần đổi các keys/password Airflow khác. `group_add` cho user Airflow nhóm bổ sung tương ứng; không đổi toàn bộ scheduler sang root để né lỗi permission.

**Checkpoint cấu hình và build**, tại workspace platform:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml config --quiet
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml build
```

Lệnh config chỉ kiểm tra cấu hình, chưa kiểm tra quyền socket, image runtime hoặc kết nối DB. Build phải pass trước khi thay các container Airflow đang dùng. Chưa copy DAG LaplapTech vào thư mục live ở bước này.

## 7. Bài 5 — cấp config/artifacts và bật warehouse

Tạo hai volume có tên ổn định. Tên này cũng được dùng trong DAG, không thêm prefix project khác:

```powershell
docker volume create laplaptech-lab-config
docker volume create laplaptech-lab-artifacts
```

Nạp file cấu hình vào volume và chuẩn bị quyền UID 10001. Đứng tại **workspace platform**; runtime image phải đã build ở mục 5. Đoạn Python chỉ copy file, không đọc dữ liệu ClickHouse/Neon.

```powershell
$configFile = (Resolve-Path -LiteralPath '.\config\.env.laplaptech').Path
$prepareVolumes = @'
import os
from pathlib import Path
config = Path('/config')
artifacts = Path('/artifacts')
config.mkdir(exist_ok=True)
artifacts.mkdir(exist_ok=True)
target = config / 'pipeline.env'
target.write_bytes(Path('/input/pipeline.env').read_bytes())
for path in (config, artifacts, target):
    os.chown(path, 10001, 10001)
os.chmod(config, 0o700)
os.chmod(artifacts, 0o700)
os.chmod(target, 0o400)
'@
docker run --rm --user 0:0 --entrypoint python --mount "type=bind,source=$configFile,target=/input/pipeline.env,readonly" --mount type=volume,source=laplaptech-lab-config,target=/config --mount type=volume,source=laplaptech-lab-artifacts,target=/artifacts laplaptech-runtime:lab-v1 -c $prepareVolumes
```

User root chỉ dùng cho bước cấp quyền volume; các task pipeline chạy UID 10001. Khi sửa `config/.env.laplaptech`, dừng các run lab rồi nạp lại bằng đoạn trên để volume nhận cấu hình mới. Nếu file cũ read-only, helper root vẫn có thể cập nhật. Không in nội dung file lên log.

Bật stack đã bổ sung; không đổi Airflow version hoặc tạo lại metadata keys:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml up -d
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml ps
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec airflow-scheduler pip check
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec airflow-scheduler python -c "import docker; print(docker.from_env().ping())"
```

Cần các service thường trực healthy, `pip check` pass và Docker ping trả về `True`. Trigger lại `platform_smoke_test` để xác nhận thay image/provider không làm hỏng bài trước.

**Checkpoint preflight — chưa ghi bảng pipeline:**

```powershell
docker run --rm --network laplaptech-lab --mount type=volume,source=laplaptech-lab-config,target=/run/laplaptech,readonly --mount type=volume,source=laplaptech-lab-artifacts,target=/artifacts laplaptech-runtime:lab-v1 preflight
```

Cần thấy `Target verified: warehouse-laplaptech / laplaptech_pipeline_dev` và exit code 0. Preflight chỉ kết nối/SELECT vào DB và ghi stage report ở artifacts; không chứng minh login ClickHouse đã đúng. Không chuyển sang ingestion nếu target check chưa đạt.

## 8. Bài 6 — tự viết DAG pipeline thật

Trong **repo LaplapTech**, tạo `infra/laplaptech/dags/laplaptech_lab.py`. Code nằm ở repo để review/version; bạn sẽ copy một file này vào platform sau checkpoint parse.

<!-- sample-file: repo/infra/laplaptech/dags/laplaptech_lab.py -->
```python
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


def pipeline_stage(stage: str, minutes: int, retries: int = 1) -> DockerOperator:
    return DockerOperator(
        task_id=stage,
        image='laplaptech-runtime:lab-v1',
        command=[stage],
        docker_url='unix://var/run/docker.sock',
        api_version='auto',
        network_mode='laplaptech-lab',
        mount_tmp_dir=False,
        mounts=[
            Mount(source='laplaptech-lab-config', target='/run/laplaptech',
                  type='volume', read_only=True),
            Mount(source='laplaptech-lab-artifacts', target='/artifacts',
                  type='volume'),
        ],
        environment={
            'PIPELINE_RUN_ID': '{{ run_id }}',
            'PIPELINE_ATTEMPT': '{{ ti.try_number }}',
        },
        labels={'com.laplaptech.project': 'laplaptech',
                'com.laplaptech.environment': 'lab'},
        user='10001:10001',
        mem_limit='2g',
        force_pull=False,
        auto_remove='never',
        do_xcom_push=False,
        pool='laplaptech_warehouse',
        retries=retries,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=minutes),
    )


with DAG(
    dag_id='laplaptech_lab',
    description='ClickHouse public -> isolated lab raw -> dbt Bronze/Silver/Gold',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Ho_Chi_Minh'),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=2),
    tags=['laplaptech', 'lab'],
) as dag:
    ingest = pipeline_stage('ingest', minutes=45)
    freshness = pipeline_stage('freshness', minutes=5, retries=0)
    build = pipeline_stage('build', minutes=30)

    ingest >> freshness >> build
```

### Đọc DAG theo ý nghĩa vận hành

| Cấu hình | Tại sao dùng ở bài này |
|---|---|
| `image` + `command=[stage]` | Image có ENTRYPOINT Python; command bổ sung tên stage, không phải câu lệnh shell lồng nhau |
| `network_mode` | Task container tham gia network có warehouse; không dùng DNS localhost của scheduler |
| Named volumes | Đường dẫn do Engine hiểu; tránh mount Windows path hoặc path chỉ tồn tại trong scheduler vào task container |
| `mount_tmp_dir=False` | Dùng volume artifacts rõ ràng, tránh temp bind mount của scheduler không có trên host Engine |
| `api_version='auto'` | Docker SDK thương lượng API với Engine, không hardcode version API Docker |
| `environment` | Chỉ truyền run/attempt qua template; login pipeline đọc từ volume bên trong task, không đưa secret vào rendered DAG |
| `do_xcom_push=False` | Không đẩy dataset hoặc toàn bộ stdout vào metadata |
| `auto_remove='never'` | Giữ task container sau khi kết thúc để học đọc lỗi; cần dọn đúng container sau khi xem xong |
| Pool 1 slot + max run/task 1 | Tuần tự hóa writer trong DAG/pool lab này; không tự khóa một writer ngoài Airflow |
| Freshness `retries=0` | Nguồn thực sự cũ không trở nên mới chỉ vì thử lại sau hai phút |
| `mem_limit='2g'` | Giới hạn RAM mỗi task container; chưa phải số đo tối ưu và không giới hạn tổng RAM Airflow/DB |
| `schedule=None`, `catchup=False` | Manual để học/đối soát trước; code này chưa triển khai bounded backfill |

DockerOperator và các tham số tham chiếu [API provider](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_api/airflow/providers/docker/operators/docker/index.html), [source provider](https://airflow.apache.org/docs/apache-airflow-providers-docker/stable/_modules/airflow/providers/docker/operators/docker.html). Các timeout/RAM trong mẫu là điểm khởi đầu; đo `docker stats` và thời gian nguồn thật trước khi chốt. Không coi pool hoặc `max_active_tasks` là giới hạn RAM toàn platform.

## 9. Bài 7 — copy DAG, tạo pool, parse rồi chạy manual

Copy **một DAG** vào platform; không ghi đè thư mục chứa smoke DAG:

```powershell
Set-Location D:\Dev\Project\laplaptech_pipeline
New-Item -ItemType Directory -Path 'D:\Dev\Project\airflow-platform\dags\laplaptech' -Force | Out-Null
Copy-Item -LiteralPath '.\infra\laplaptech\dags\laplaptech_lab.py' -Destination 'D:\Dev\Project\airflow-platform\dags\laplaptech\laplaptech_lab.py'
Set-Location D:\Dev\Project\airflow-platform
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec airflow-scheduler airflow pools set laplaptech_warehouse 1 'One writer for the LaplapTech lab warehouse'
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec airflow-dag-processor airflow dags list-import-errors
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec airflow-scheduler airflow dags list
```

Chờ processor parse file mới. Phải **thấy `laplaptech_lab` trong danh sách** và không có import error; chỉ thấy `No data found` ở import errors chưa chứng minh DAG đã xuất hiện.

Trong UI: mở `laplaptech_lab` → unpause → Trigger manual. Đây là lần bắt đầu đọc nguồn public và ghi warehouse lab. Ba task phải chạy lần lượt; nếu ingest fail, freshness/build bị chặn; nếu freshness error, build chưa chạy. Warning freshness chưa nhất thiết làm task fail: xem exit code và artifact.

### Freshness có thể chặn một nguồn public cũ

Repo đang dùng server event timestamp: warn sau 24 giờ, error sau 48 giờ. **Ingest chạy hôm nay không làm event lịch sử trở thành event mới.** Nếu nguồn public không còn cập nhật, kiểm tra `sources.json` và timestamp thực tế; task fail có thể là đúng contract, không phải Airflow hỏng. Không xóa dependency freshness hoặc đánh success thủ công để đạt DAG xanh. Nếu mục tiêu đổi sang nghiên cứu dataset lịch sử, cần thay contract freshness riêng có giải thích và kiểm thử, hoặc thử build độc lập trong lab sau khi đã ghi nhận nguồn stale; không sửa contract cloud trong bài này.

## 10. Bài 8 — kiểm chứng kết quả, đọc artifacts và thử lỗi

Trong workspace platform, sau run:

```powershell
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml exec -T warehouse-laplaptech psql -U laplaptech -d laplaptech_pipeline_dev -c "SELECT (SELECT count(*) FROM raw.user_event_tracking) AS raw_events, (SELECT count(*) FROM public_silver.silver_user_event_tracking) AS silver_events, (SELECT sum(total_events) FROM public_gold.gold_daily_site_kpis) AS gold_daily_events;"
docker run --rm --mount type=volume,source=laplaptech-lab-artifacts,target=/artifacts,readonly --entrypoint python laplaptech-runtime:lab-v1 -c 'from pathlib import Path; print("\n".join(str(p) for p in Path("/artifacts").rglob("report.json")))'
docker ps -a --filter label=com.laplaptech.project=laplaptech
```

Kỳ vọng `gold_daily_events` khớp `silver_events`; raw có thể lớn hơn Silver vì lọc event/session thiếu hoặc dedup. Data tests trong `dbt build` còn kiểm tra ID/version và nhiều metric; một câu COUNT không thay thế toàn bộ đối soát.

Mỗi stage in `Stage artifacts: ...` trong task log. `freshness/.../target/sources.json` và `build/.../target/manifest.json`, `run_results.json` được giữ riêng trong volume; không bị task sau ghi đè. Để đọc một report, thay đường dẫn cụ thể đã lấy từ log:

```powershell
docker run --rm --mount type=volume,source=laplaptech-lab-artifacts,target=/artifacts,readonly --entrypoint python laplaptech-runtime:lab-v1 -c 'from pathlib import Path; print(Path("/artifacts/REPLACE_WITH_RUN_KEY/build/REPLACE_WITH_ATTEMPT_FOLDER/report.json").read_text())'
```

**Bài tập lỗi:** sửa **bản config lab** thành tên DB sai, nạp lại volume, trigger run mới. Guard phải chặn trước ingestion. Sau đó sửa lại, nạp config và trigger run mới. Làm khi không có run đang chạy; không thử bằng cách đổi sang một endpoint Neon thật.

**Bài tập replay:** sau run xanh, trigger run thứ hai với nguồn không đổi; kiểm tra duplicates, ID/version, dbt tests và metrics. Nguồn live có thể thay đổi giữa hai run nên không mặc định row count phải giữ nguyên. Muốn test replay bất biến dùng fixture cố định trong bộ kiểm thử hiện có. Clear/retry không rollback các bảng đã commit; đây là lý do phải quan sát transaction boundaries và đối soát.

**Bài tập restart:** dừng/bật lại bằng cùng hai file Compose, xác nhận login, lịch sử run, warehouse và logs vẫn còn. Trong lúc có run đang chạy, đợi nó kết thúc trước khi làm bài restart.

## 11. Vận hành sau khi thêm file Compose bổ sung

Các lệnh từ đây phải chỉ rõ **cả hai file**. Nếu chỉ dùng base, bạn có thể vô tình quay về image Airflow chưa có provider.

```powershell
Set-Location D:\Dev\Project\airflow-platform
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml stop
docker compose --env-file .env -f compose.yaml -f compose.laplaptech.yaml up -d
```

Containers task do DockerOperator tạo không thuộc lifecycle Compose; pool/DAG run phải hoàn tất trước khi stop platform. Task container đã `Exited` không dùng RAM như container đang chạy, nhưng vẫn chiếm disk. Xem danh sách qua label ở mục 10 rồi xóa **ID container cụ thể đã kết thúc** bằng `docker rm <ID>` sau khi kiểm tra artifacts/logs. Không dùng global prune hoặc xóa volumes để dọn task container.

| Lỗi | Kiểm tra đúng lớp |
|---|---|
| ImportError DockerOperator | Image provider trên cả scheduler/API/DAG processor, `pip check`, đúng hai file Compose |
| Permission denied docker.sock | GID socket và `group_add`; cần recreate scheduler sau khi sửa group |
| Image not found | Build runtime trên đúng Docker Engine/context mà scheduler đang dùng, tag trong DAG khớp |
| Could not resolve warehouse-laplaptech | Warehouse healthy, network có đúng tên `laplaptech-lab`, task có `network_mode` |
| Missing config / permission artifacts | Volume có đúng tên, đã nạp config, UID 10001 có quyền; không chmod 777 để bỏ qua nguyên nhân |
| DB auth failed | Password config và password khi warehouse init có khớp; thay env không đổi password DB đã có |
| Lab guard failed | Giữ đúng đích lab; không xóa guard để cho task chạy |
| Freshness fail | Kiểm tra timestamp nguồn và `sources.json`, phân biệt nguồn stale với lỗi kết nối |
| Task queued | Scheduler healthy, DAG unpaused, pool tồn tại và có slot, run cũ đã kết thúc |
| Exit 137 / timeout | Đo RAM/thời gian từng stage, tài nguyên Docker còn đủ; tăng hạn mức có căn cứ |

## 12. Khi nào coi bài tích hợp đạt?

- [ ] Runtime image và image Airflow có provider build được, `pip check` pass.
- [ ] Smoke DAG cũ vẫn success, scheduler truy cập được Engine.
- [ ] Preflight đúng DB lab; thử sai tên DB bị chặn trước ghi dữ liệu.
- [ ] DAG LaplapTech parse được, pool hoạt động, ba task chạy đúng thứ tự.
- [ ] Nguồn có contract freshness rõ; nếu stale thì failure được giải thích bằng artifact, chưa đánh dấu toàn pipeline xanh.
- [ ] Build/data tests và đối soát raw → Gold đạt trên nguồn/fixture đã ghi nhận.
- [ ] Logs/artifacts đọc được theo run/stage/attempt, kể cả task lỗi.
- [ ] Replay/failure recovery và restart giữ dữ liệu đã được thực hành.
- [ ] Neon + lịch Actions tiếp tục độc lập, không có writer lab trỏ về Neon.

Bạn đủ hiểu bài này khi tự trả lời được: vì sao cần hai DB; ai tạo task container; tại sao không dùng localhost; vì sao không truyền password trong DAG; sửa Python cần rebuild image nào; và vì sao DAG xanh chưa thay thế kiểm thử dữ liệu.

Sau bài này mới làm audit/backfill/recovery sâu hơn và đóng gói Docker full cho cộng đồng theo [backlog](../architecture/remaining_work.md), [Docker distribution plan](docker_distribution_plan.md). Mẫu hiện chưa cấp dashboard container, lịch production, bounded backfill hoặc thông báo lỗi tự động.
