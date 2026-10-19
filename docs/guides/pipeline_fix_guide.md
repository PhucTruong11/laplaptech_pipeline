# Hướng dẫn sửa và kiểm chứng LaplapTech Pipeline

Ngày viết: 19/10/2026. Đây là hướng dẫn tổng thể và code mẫu để tự triển khai.

**Cập nhật:** ingestion, workflow và một phần tests đã được triển khai sau khi viết hướng dẫn. Xem [hướng dẫn vận hành ingestion](ingestion_operations.md) và source hiện tại để biết hành vi thực tế; không copy lại mẫu cũ để ghi đè các bản sửa mới. Các phần dashboard/Airflow vẫn là hướng dẫn tiếp theo.

**Sơ đồ triển khai hiện tại:** [data flow](../architecture/data_flow.md). **Việc còn thiếu có ưu tiên:** [remaining work](../architecture/remaining_work.md). Các snippet dưới đây giải thích cách sửa, không phải toàn bộ source cuối cùng và không phải checklist đã hoàn thành hết.

| Phần trong hướng dẫn | Trạng thái thực tế ngày 19/10/2026 |
|---|---|
| Transaction, dedup, staging, watermark, URL kết nối | Đã triển khai; source có thêm kiểm tra xung đột version giữa batch/raw |
| CLI backfill start/end | Đã triển khai; còn phải xử lý Silver ngoài lookback khi nạp bù lịch sử |
| Workflow sync và CI | Đã triển khai; CI dùng requirements.txt hiện có, chưa có dependency lock |
| Daily KPI và data tests | Có SQL mới; 34 data test đã parse, chưa xác nhận pass trên Neon sau sửa |
| Python tests | Source thực tế có 21 test đã pass local, gồm 9 PostgreSQL integration test |
| Dashboard, timezone toàn bộ, audit, reconciliation | Chưa hoàn tất; xem backlog |
| Airflow Docker | Kế hoạch học tiếp; DAG mẫu chưa được triển khai |

## 1. Quyết định trước khi sửa

**Khuyến nghị theo mục tiêu học:** sửa các lỗi metric và kiểm chứng dữ liệu, sau đó dùng Airflow Docker local để thực hành orchestration. Trong lúc chưa có runtime/DAG đã kiểm thử, GitHub Actions vẫn chạy lịch hiện tại. Có thể giữ Neon làm warehouse; chưa cần VPS nếu chấp nhận lab chỉ chạy khi laptop/Docker bật.

| Thứ tự | Việc cần làm | Hoàn thành khi |
|---|---|---|
| 1 | Ngăn job chạy chồng, tách dev khỏi warehouse đang phục vụ | Chỉ `main` được ghi warehouse chính; các lượt chạy cùng warehouse không chạy song song |
| 2 | Sửa full refresh thành transaction duy nhất | Insert lỗi thì dữ liệu cũ vẫn còn |
| 3 | Dedup đúng phiên bản và thay staging dùng chung | Giữ timestamp mới nhất; chạy lại không tăng bản ghi; staging được dọn trong transaction |
| 4 | Chuẩn hóa grain, timezone và daily KPI | `total_events` đối soát được với bảng event; chỉ số có định nghĩa rõ |
| 5 | Sửa nhãn và dữ liệu thiếu trên dashboard | Không gọi ngày dữ liệu cũ là Today; không dựng số liệu thay thế |
| 6 | Bổ sung data tests và integration tests | Có test cho khóa event, idempotency, rollback và đối soát |
| 7 | Chạy freshness, lưu artifact và khóa dependency | Job có kết quả freshness; môi trường cài đặt tái lập được |
| 8 | Sửa lại tài liệu và đánh giá Airflow lab | Phân biệt số model, số test, giới hạn lookback và khả năng backfill |

Các snippet ghi **thay toàn bộ** phải thay phần cũ tương ứng, tránh giữ hai định nghĩa hàm/model. Các đường dẫn code trong tài liệu tính từ thư mục gốc repository.

## 2. Bước 0 — Chuẩn bị môi trường sửa và database test

1. Kiểm tra `git diff` và lưu lại thay đổi đang làm; không dùng reset/checkout để xóa công việc chưa commit.
2. Tạo branch riêng nếu cần: `git switch -c codex/pipeline-reliability`.
3. Trong PowerShell ở thư mục gốc, kích hoạt venv và kiểm tra phiên bản:

```powershell
.\venv\Scripts\Activate.ps1
python --version
python -m pip check
dbt --version
```

4. Dùng PostgreSQL test riêng. Nếu máy đã cài Docker, có thể tạo instance local:

```powershell
docker run --name laplaptech-test-pg -e POSTGRES_PASSWORD=local_test_only -e POSTGRES_DB=laplaptech_pipeline_test -p 127.0.0.1:55432:5432 -d postgres:16
$env:TEST_DATABASE_URL = 'postgresql+psycopg2://postgres:local_test_only@localhost:55432/laplaptech_pipeline_test'
docker exec laplaptech-test-pg pg_isready -U postgres -d laplaptech_pipeline_test
```

Nếu container chưa ready, chờ rồi chạy lại `pg_isready`. Các test ở mục 8 chỉ dùng URL này; không dùng URL Neon chính. Password trên chỉ dành cho instance local ví dụ.

5. Khi chạy dbt lên warehouse dev, dùng database dev có đủ raw tables hoặc bản sao dữ liệu đã chuẩn bị. Chỉ đổi `schema` dbt chưa cô lập ingestion vì Python vẫn ghi schema `raw` của database được cấu hình.

dbt không tự đọc file `.env` của Python. Có thể nạp `.env` cho từng lệnh bằng CLI của python-dotenv đã có trong dependencies:

```powershell
python -m dotenv -f .env run -- dbt parse --project-dir dbt --profiles-dir dbt
```

Lệnh parse không chạy model lên database. Các lệnh `build`, `run`, ingestion ở phần sau có ghi dữ liệu: chỉ chạy khi cấu hình đang trỏ đúng môi trường muốn kiểm thử.

## 3. Bước 1 — Chặn chạy chồng và tách CI khỏi job ghi dữ liệu

### 3.1 Sửa trigger của workflow sync

Trong `.github/workflows/sync-pipeline.yml`, đổi phần `on` và thêm `concurrency` ở cấp root:

```yaml
on:
  push:
    branches: [main]
  schedule:
    - cron: "15 1 * * *"   # 08:15 giờ Việt Nam
    - cron: "45 9 * * *"   # 16:45 giờ Việt Nam
  workflow_dispatch:

concurrency:
  group: laplaptech-production-warehouse
  cancel-in-progress: false

permissions:
  contents: read
```

Group cố định theo warehouse, không thêm `${{ github.ref }}`: hai branch dùng chung DB vẫn phải bị chặn chạy song song. Đây là chống chạy đồng thời; không phải hàng đợi bảo đảm xử lý mọi trigger.

Trong job `sync-and-transform`, thêm điều kiện sau để manual dispatch trên branch khác không ghi vào warehouse chính:

```yaml
    if: github.ref == 'refs/heads/main'
```

Các workflow khác cũng ghi cùng warehouse phải dùng cùng group. Concurrency của GitHub không chặn được một script bạn chạy trên laptop; các lock ở bước 3 chỉ bảo vệ từng thao tác nạp bảng, không khóa toàn bộ ingestion + dbt.

### 3.2 CI cho PR/develop chỉ kiểm tra offline

Tạo `.github/workflows/validate.yml`. Workflow này không cần secrets và không chạy ingestion:

```yaml
name: Validate pipeline code
on:
  pull_request:
  push:
    branches: [develop]
permissions:
  contents: read
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.13"
      - run: pip install -r requirements.lock.txt
      - run: python -m compileall -q ingestion streamlit_app
      - run: dbt parse --project-dir dbt --profiles-dir dbt
        env:
          POSTGRES_HOST: localhost
          POSTGRES_PORT: "5432"
          POSTGRES_USER: parse_only
          POSTGRES_PASSWORD: parse_only
          POSTGRES_DATABASE: parse_only
          DBT_SEND_ANONYMOUS_USAGE_STATS: "false"
```

Tạo `requirements.lock.txt` theo bước 7 trước khi bật workflow này. `dbt parse` chỉ xác nhận cấu hình/Jinja và dependency graph; không xác nhận SQL chạy đúng hay dữ liệu đạt test.

**Kiểm chứng:** push lên `develop` chỉ chạy validate. Trigger hai lượt sync trên `main`: lượt sau không được chạy cùng lúc với lượt trước.

## 4. Bước 2 và 3 — Sửa ingestion

### 4.1 Thêm các helper

Trong `ingestion/clickhouse_to_postgres.py`, thêm import:

```python
import math
from uuid import uuid4
from sqlalchemy import inspect
from sqlalchemy.engine import URL
```

Thêm các helper sau trước hai hàm load. Các cột epoch của dataset hiện tại được xử lý theo giây dạng số; nếu nguồn đổi sang datetime phải đổi cả extraction và chuẩn hóa watermark.

```python
def quote_ident(conn, name: str) -> str:
    return conn.dialect.identifier_preparer.quote_identifier(name)


def raw_relation(conn, table_name: str) -> str:
    return f"{quote_ident(conn, RAW_SCHEMA)}.{quote_ident(conn, table_name)}"


def lock_raw_table(conn, table_name: str):
    # Mọi writer dùng helper này sẽ tuần tự hóa thao tác trên cùng bảng.
    conn.execute(text("SET LOCAL lock_timeout = '30s'"))
    conn.execute(
        text("SELECT pg_advisory_xact_lock(74621, hashtext(:resource))"),
        {"resource": f"{RAW_SCHEMA}.{table_name}"},
    )


def assert_same_columns(conn, table_name: str, df: pd.DataFrame):
    columns = inspect(conn).get_columns(table_name, schema=RAW_SCHEMA)
    existing = {column["name"] for column in columns}
    incoming = set(df.columns)
    if existing != incoming:
        raise ValueError(
            f"Schema thay đổi ở {table_name}: "
            f"thiếu={existing - incoming}, thừa={incoming - existing}"
        )


def prepare_event_batch(
    df: pd.DataFrame,
    primary_key: str = "id",
    timestamp_col: str = "event_received_on_server_timestamp",
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    batch = df.copy()
    if batch[primary_key].isna().any() or batch[timestamp_col].isna().any():
        raise ValueError("Event thiếu ID hoặc watermark; cần kiểm tra nguồn")
    batch[timestamp_col] = pd.to_numeric(batch[timestamp_col], errors="raise")
    if not batch[timestamp_col].map(math.isfinite).all():
        raise ValueError("Watermark phải là epoch hữu hạn")

    # Tìm timestamp lớn nhất cho mỗi ID, không dựa vào thứ tự query nguồn.
    latest = (
        batch.sort_values(timestamp_col, kind="stable")
        .drop_duplicates(primary_key, keep="last")[[primary_key, timestamp_col]]
    )
    candidates = batch.merge(latest, on=[primary_key, timestamp_col])
    candidates = candidates.drop_duplicates()
    if candidates.duplicated(primary_key).any():
        raise ValueError(
            "Cùng ID và timestamp mới nhất nhưng payload khác nhau; "
            "cần version/ingested_at từ nguồn để phân xử"
        )
    return candidates.reset_index(drop=True)
```

`drop_duplicates()` toàn dòng giả định cột JSON đang là chuỗi như cách model hiện tại cast `::json`. Nếu ClickHouse client trả dict/list, serialize JSON nhất quán trước khi gọi helper. Không dùng vị trí dòng trả về để chọn phiên bản.

`assert_same_columns` phát hiện thêm/bớt cột; đây chưa phải kiểm tra đầy đủ kiểu dữ liệu. Type không hợp lệ sẽ bị PostgreSQL từ chối và transaction rollback. Về lâu dài nên quản lý schema raw bằng DDL thay vì chỉ dựa vào pandas inference.

### 4.2 Thay toàn bộ `load_to_postgres`

Chính sách mẫu: dimension rỗng là lỗi cần điều tra, giữ dữ liệu cũ. Nếu nghiệp vụ thật sự cho phép xóa sạch một dimension, thiết kế thao tác riêng có chủ ý.

```python
def load_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    engine,
    if_exists: str = "append",
    truncate_first: bool = False,
):
    if table_name not in FULL_REFRESH_TABLES:
        raise ValueError(f"Không phải bảng full refresh: {table_name}")
    if if_exists != "append":
        raise ValueError("Loader chỉ hỗ trợ append trong transaction")
    if df.empty:
        raise ValueError(f"Nguồn {table_name} rỗng; giữ nguyên dữ liệu cũ")

    with engine.begin() as conn:
        lock_raw_table(conn, table_name)
        exists = inspect(conn).has_table(table_name, schema=RAW_SCHEMA)
        if exists:
            assert_same_columns(conn, table_name, df)
        if truncate_first and exists:
            conn.execute(text(f"TRUNCATE TABLE {raw_relation(conn, table_name)}"))
        df.to_sql(
            table_name,
            con=conn,  # Quan trọng: cùng Connection, không truyền engine.
            schema=RAW_SCHEMA,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
    logger.info("Đã commit %s: %s dòng", table_name, len(df))
```

Điểm quyết định là `con=conn` trong cùng `engine.begin()`: truncate và mọi batch insert cùng commit hoặc cùng rollback. Không bắt exception rồi bỏ qua. Chunk nhỏ hơn giúp giảm kích thước mỗi câu INSERT; vẫn cần đo thực tế để tối ưu.

Mẫu này atomic theo **mỗi bảng**, chưa tạo snapshot đồng bộ cho toàn bộ năm dimension. Nếu job dừng giữa các bảng, một số bảng đã mới và một số còn cũ; giữ job dbt phía sau chỉ chạy khi ingestion thành công, và retry cả ingestion. Nếu cần snapshot nhất quán giữa mọi bảng, phải stage toàn bộ rồi publish trong transaction chung.

### 4.3 Thay toàn bộ `load_incremental_to_postgres`

Giữ cách gọi trong `run_pipeline()` như hiện tại. Staging dưới đây có tên riêng, được tạo, nạp và drop trong một transaction. Nếu lỗi/kết nối bị mất trước commit, PostgreSQL rollback cả staging và thay đổi raw khi transaction bị hủy.

```python
def load_incremental_to_postgres(
    df: pd.DataFrame, table_name: str, engine, primary_key: str = "id"
):
    if table_name not in INCREMENTAL_TABLES:
        raise ValueError(f"Không phải bảng incremental: {table_name}")
    ts_col = INCREMENTAL_TABLES[table_name]
    batch = prepare_event_batch(df, primary_key, ts_col)
    if batch.empty:
        logger.info("Không có event mới: %s", table_name)
        return

    with engine.begin() as conn:
        lock_raw_table(conn, table_name)
        exists = inspect(conn).has_table(table_name, schema=RAW_SCHEMA)
        if not exists:
            batch.to_sql(
                table_name, con=conn, schema=RAW_SCHEMA,
                if_exists="append", index=False,
                chunksize=1000, method="multi",
            )
        else:
            assert_same_columns(conn, table_name, batch)
            target = raw_relation(conn, table_name)
            stg_name = f"_stg_{table_name}_{uuid4().hex[:12]}"
            staging = raw_relation(conn, stg_name)
            pk = quote_ident(conn, primary_key)
            ts = quote_ident(conn, ts_col)
            columns = ", ".join(quote_ident(conn, c) for c in batch.columns)
            conn.execute(text(f"CREATE TABLE {staging} (LIKE {target})"))
            batch.to_sql(
                stg_name, con=conn, schema=RAW_SCHEMA,
                if_exists="append", index=False,
                chunksize=1000, method="multi",
            )

            # Batch đến sau không được thay bản mới hơn trong raw bằng bản cũ.
            conn.execute(text(f"""
                DELETE FROM {staging} s USING {target} t
                WHERE s.{pk} = t.{pk} AND s.{ts} < t.{ts}
            """))
            conn.execute(text(f"""
                DELETE FROM {target} t USING {staging} s
                WHERE t.{pk} = s.{pk}
            """))
            conn.execute(text(f"""
                INSERT INTO {target} ({columns})
                SELECT {columns} FROM {staging}
            """))
            conn.execute(text(f"DROP TABLE {staging}"))
    logger.info("Đã commit batch %s: %s ID đầu vào", table_name, len(batch))
```

Mẫu này giả định ID là định danh event ổn định và nguồn không sửa payload với timestamp giữ nguyên. Helper phát hiện xung đột timestamp trong cùng batch; nếu payload được sửa giữa các batch mà timestamp không đổi, cần thêm version/updated_at và quy tắc so sánh phiên bản. Không thể suy ra bản đúng chỉ từ ID + timestamp bằng nhau.

Lock là advisory: writer ngoài project không dùng cùng lock vẫn có thể ghi. Sau khi kiểm tra/dọn duplicate có sẵn, nên thêm unique index cho ID để DB cũng bảo vệ invariant. Trước hết chạy trên database test/dev:

```sql
SELECT id, COUNT(*)
FROM raw.user_event_tracking
GROUP BY id
HAVING id IS NULL OR COUNT(*) > 1;

-- Chỉ chạy khi query trên không trả dòng và đã xử lý dữ liệu cũ.
ALTER TABLE raw.user_event_tracking ALTER COLUMN id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_user_event_id
ON raw.user_event_tracking (id);

CREATE INDEX IF NOT EXISTS ix_raw_user_event_watermark
ON raw.user_event_tracking (event_received_on_server_timestamp);
```

Không xóa bản duplicate tùy tiện nếu cùng ID có payload khác nhau. Đối chiếu nguồn để giữ đúng phiên bản. Raw upsert ở trên không phải lịch sử bất biến; muốn lưu mọi phiên bản cần bảng history riêng.

### 4.4 Sửa fallback watermark và connection URL

Trong `extract_incremental()`, thay khối `try/except Exception: pass` khi đọc MAX bằng:

```python
max_ts = None
with engine.connect() as conn:
    if inspect(conn).has_table(table_name, schema=RAW_SCHEMA):
        column = quote_ident(conn, timestamp_col)
        relation = raw_relation(conn, table_name)
        max_ts = conn.execute(
            text(f"SELECT MAX({column}) FROM {relation}")
        ).scalar()
```

Giữ phần dựng query lookback phía sau. Chỉ bảng chưa tồn tại mới full load; lỗi quyền/kết nối không nên bị biến thành full extraction âm thầm. Cũng nên đổi `if max_ts:` thành `if max_ts is not None:`.

Trong `get_postgres_engine()` và `get_engine()` của dashboard, thay URL ghép chuỗi bằng `URL.create` để password chứa `@`, `/`, `:` không làm sai URL:

```python
url = URL.create(
    "postgresql+psycopg2",
    username=user, password=password, host=host,
    port=int(port), database=database,
)
return create_engine(url, pool_pre_ping=True)
```

Dashboard cũng cần import `URL`. Với `ensure_database_exists()`, áp dụng cùng cách tạo URL cho database `postgres`; khi dùng dịch vụ managed và database đã được cấp sẵn, cân nhắc tách provisioning ra khỏi job sync. Không tự thử tạo database trong mỗi lần đồng bộ nếu tài khoản không có quyền.

### 4.5 Lookback: ghi rõ giới hạn và đường sửa dữ liệu cũ

Hiện ingestion mặc định lấy từ `MAX(raw timestamp) - 2 giờ` qua `INGESTION_LOOKBACK_SECONDS=7200`, Silver lấy từ `MAX(silver timestamp) - 2 giờ`. Raw và Silver có hai watermark riêng; cửa sổ Silver không lấy lại được bản ghi mà ingestion chưa tải về.

- Event xảy ra từ lâu nhưng **server nhận bây giờ** vẫn được lấy nếu watermark là giờ server nhận mới.
- Bản ghi chỉ xuất hiện/cập nhật ở nguồn sau đó nhưng mang server timestamp cũ hơn cửa sổ ingestion có thể bị bỏ sót.
- Một bản đã có trong raw nhưng nằm ngoài cửa sổ Silver có thể cần rebuild/backfill Silver.

Đo độ trễ thực tế để chọn window; không chỉ tăng số giờ rồi khẳng định không mất dữ liệu. Muốn backfill theo lịch Airflow sau này, phải hỗ trợ `--start`/`--end` cho extraction và chọn khoảng `[start, end)` rõ ràng.

Ví dụ query nguồn cho một khoảng epoch giây, không dùng MAX đích:

```python
query = f"""
    SELECT * FROM {table_name}
    WHERE {timestamp_col} >= {{start:Float64}}
      AND {timestamp_col} < {{end:Float64}}
"""
result = ch_client.query(query, parameters={"start": start_epoch, "end": end_epoch})
```

CLI `--start`/`--end` hiện đã có trong source; query ở đây minh họa cách giới hạn khoảng. Table/column phải lấy từ allowlist hiện tại. Khi backfill: upsert raw khoảng bị thiếu → rebuild model event Silver cùng downstream trong dev → đối soát trước khi chạy trên warehouse chính. `dbt --full-refresh` chỉ đọc raw hiện có, không tự kéo lại ClickHouse. Xem [lệnh backfill thực tế](ingestion_operations.md).

## 5. Bước 4 — Sửa daily KPI và định nghĩa funnel

### 5.1 Chọn grain trước khi viết SQL

Đề xuất cho `mart_daily_site_kpis`:

| Trường | Định nghĩa |
|---|---|
| `event_date` | Ngày event được server nhận, theo `Asia/Ho_Chi_Minh` |
| `total_events` | Số dòng event hợp lệ trong Silver của ngày đó |
| `total_sessions` | Số session hoạt động trong ngày; session qua nửa đêm tính ở cả hai ngày |
| `sessions_with_*` | Số session có hành vi tương ứng trong ngày |
| `detail_rate_pct` | Session có pageview và detail / session có pageview |
| `comparison_rate_pct` | Session có cả detail và comparison / session có detail |

Hai rate trên là **co-reach** trong cùng ngày, chưa bảo đảm thứ tự. All-time SUM của daily active sessions là **session-days**, không phải unique sessions toàn thời gian.

### 5.2 Thay toàn bộ `dbt/models/gold/mart_daily_site_kpis.sql`

```sql
WITH session_day AS (
    SELECT
        (server_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS event_date,
        session_id,
        COUNT(*) AS event_count,
        BOOL_OR(event_name = 'pageview') AS has_pageview,
        BOOL_OR(event_name = 'pageview' AND page_name = 'DeviceDetail') AS has_detail,
        BOOL_OR(event_name = 'pageview' AND page_name = 'CompareDevice') AS has_comparison,
        BOOL_OR(
            event_name = 'click' AND page_name = 'CompareDevice'
            AND NULLIF(TRIM(sort_by), '') IS NOT NULL
        ) AS has_sort
    FROM {{ ref('silver_user_event_tracking') }}
    GROUP BY 1, 2
), daily AS (
    SELECT
        event_date,
        COUNT(*) AS total_sessions,
        SUM(event_count)::bigint AS total_events,
        COUNT(*) FILTER (WHERE has_pageview) AS sessions_with_pageview,
        COUNT(*) FILTER (WHERE has_detail) AS sessions_with_detail_view,
        COUNT(*) FILTER (WHERE has_comparison) AS sessions_with_comparison,
        COUNT(*) FILTER (WHERE has_sort) AS sessions_with_sort,
        COUNT(*) FILTER (WHERE has_pageview AND has_detail) AS pageview_and_detail,
        COUNT(*) FILTER (WHERE has_detail AND has_comparison) AS detail_and_comparison
    FROM session_day
    GROUP BY event_date
)
SELECT
    event_date, total_sessions, total_events,
    sessions_with_pageview, sessions_with_detail_view,
    sessions_with_comparison, sessions_with_sort,
    ROUND(100.0 * pageview_and_detail / NULLIF(sessions_with_pageview, 0), 2) AS detail_rate_pct,
    ROUND(100.0 * detail_and_comparison / NULLIF(sessions_with_detail_view, 0), 2) AS comparison_rate_pct
FROM daily
```

Không nhân events với sessions bằng cách join hai bảng ở grain khác nhau rồi COUNT. CTE `session_day` gom trước, `daily` mới tổng hợp tiếp.

**Thay đổi nghiệp vụ:** model cũ gán cả session về ngày bắt đầu, model mới đo hoạt động theo ngày. Ghi rõ thay đổi này trong README/dashboard; không so sánh số mới với số cũ như cùng một metric.

### 5.3 Giữ funnel reach hay chuyển sequential?

Đối với project hiện tại, có thể giữ `silver_session_funnel` và `mart_behavior_funnel_daily` như báo cáo **session step reach**. Đổi label thành “Session step reach”, ghi “không yêu cầu thứ tự; ngày bắt đầu session”. Đổi biểu thức ngày trong `silver_session_funnel` thành:

```sql
(MIN(server_timestamp) AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS event_date
```

Không dùng reach funnel để kết luận bao nhiêu người đi lần lượt từ bước 1 đến bước 5. Nếu cần sequential funnel, tạo model riêng: tìm pageview đầu tiên, detail đầu tiên **sau** pageview, selection sau detail, comparison sau selection, sort sau comparison; quy định timeout và timestamp bằng nhau. Chỉ `MIN()` độc lập từng loại event không đủ khi một hành vi lặp lại trước và sau bước trước đó.

Các mart CPU/GPU đang `DATE_TRUNC` theo timezone session DB; đổi thành `DATE_TRUNC('month', evt.server_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')::date` nếu muốn cùng timezone nghiệp vụ. Áp dụng tương tự cho các ngày đang cast trực tiếp ở `silver_device_traffic_event`, `silver_comparison_sort_event`, `silver_comparison_session_device`. Giữ timestamp gốc có timezone ở Silver.

## 6. Bước 5 — Sửa dashboard để không gây hiểu nhầm

### 6.1 Today phải là ngày hiện tại

Trong nhánh Today's metrics của `streamlit_app/app.py`, thay query `WITH recent_dates ...` bằng:

```python
kpi_df = safe_query("""
    WITH clock AS (
        SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS today
    )
    SELECT
        COALESCE(MAX(k.total_sessions) FILTER (WHERE k.event_date = c.today), 0) AS today_sessions,
        COALESCE(SUM(k.total_sessions) FILTER (WHERE k.event_date >= c.today - 7 AND k.event_date < c.today), 0) AS week_sessions,
        COALESCE(MAX(k.sessions_with_detail_view) FILTER (WHERE k.event_date = c.today), 0) AS today_details,
        COALESCE(SUM(k.sessions_with_detail_view) FILTER (WHERE k.event_date >= c.today - 7 AND k.event_date < c.today), 0) AS week_details,
        COALESCE(MAX(k.sessions_with_comparison) FILTER (WHERE k.event_date = c.today), 0) AS today_comps,
        COALESCE(SUM(k.sessions_with_comparison) FILTER (WHERE k.event_date >= c.today - 7 AND k.event_date < c.today), 0) AS week_comps,
        MAX(k.event_date) AS latest_data_date
    FROM clock c
    LEFT JOIN public_gold.mart_daily_site_kpis k ON TRUE
""")
```

Thêm `from datetime import datetime` và `from zoneinfo import ZoneInfo` ở đầu app. Sau khi có `row`, thêm:

```python
latest = row.get("latest_data_date")
today_vn = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()
if pd.isna(latest) or latest < today_vn:
    st.warning(f"Chưa có event của hôm nay. Ngày dữ liệu gần nhất: {latest}")
else:
    st.caption(f"Ngày dữ liệu gần nhất: {latest}; pipeline cập nhật hai lần/ngày.")
```

Đây là kiểm tra ngày có event, không phải bằng chứng job vừa chạy. Không có event mới có thể do ít traffic hoặc ingestion lỗi; muốn phân biệt cần lưu `last_successful_run_at`/watermark trong bảng audit. Không ghi “đồng bộ xong lúc…” từ MAX(event_date).

Giữ phép chia `week_sessions / 7` chỉ khi 7 ngày trước đã đủ coverage. Nếu dataset mới có ít ngày, hoặc đang giữa ngày, đừng diễn giải biến động là tăng/giảm kinh doanh chắc chắn; ghi “so với trung bình 7 ngày trước; hôm nay chưa đủ ngày”. Không tính hôm nay vào baseline 7 ngày.

Khi query lỗi và `safe_query` trả DataFrame rỗng, hiện thông báo không lấy được dữ liệu; không đổi lỗi kết nối thành KPI 0. Chỉ query thành công mà không có event mới hiển thị 0 như mẫu trên.

### 6.2 Không dùng số pin mặc định `8.8`

Thay dòng gán `avg_battery` và điều chỉnh cả hai chỗ render card dùng nó:

```python
avg_battery = None
if not battery_kpi.empty and pd.notna(battery_kpi.iloc[0]["avg_battery"]):
    avg_battery = float(battery_kpi.iloc[0]["avg_battery"])
battery_label = f"{avg_battery:.1f} hrs" if avg_battery is not None else "N/A"
# Trong card Today:
render_kpi_card("Avg Battery Life (All)", battery_label, "Benchmark fleet avg", "neutral")
# Trong card All-Time, badge dùng battery_label thay vì f"{avg_battery}h...".
```

Các cột benchmark thiếu dữ liệu trong catalog cũng nên giữ NULL/N/A thay vì `COALESCE(..., 0)` nếu 0 có thể bị hiểu là kết quả đo.

### 6.3 All-time, search và chart

Sau đổi grain của daily KPI, hoặc đổi nhãn “Total Sessions” thành “Total Session-days”, hoặc lấy unique session từ `public_silver.silver_user_event_tracking` bằng `COUNT(DISTINCT session_id)`. Tương tự, “Total Detail Views” hiện đang là số session có detail, không phải số lượt xem: đổi nhãn hoặc đổi query để đếm event.

Sửa filter search để ký tự `[` không bị coi là regex:

```python
catalog_df = catalog_df[
    catalog_df["laptop_name"].str.contains(search_query, case=False, na=False, regex=False)
]
```

Ở query search analytics, thêm `ORDER BY search_volume DESC, search_keyword` trước `LIMIT 25`. Thứ tự vật lý của bảng không bảo đảm kết quả top-N.

“Top Search Queries” chưa chứng minh “Product Gaps”: muốn kết luận nhu cầu chưa đáp ứng cần thêm search result count/zero-result và mapping catalog. Đổi label về “Top Search Queries” trong lúc chưa có dữ liệu đó.

Chart “Key Metrics (Monthly Trend)” đang lấy mart CPU có lọc nhóm ít hơn 5 view và CPU thiếu tên. Nếu muốn tổng traffic, tổng hợp từ daily KPI mới; nếu giữ query cũ thì ghi rõ phạm vi. Donut chỉ lấy top 6 brand cần nhãn “Share among top 6 brands” hoặc thêm nhóm Other để biểu diễn share toàn bộ.

**Kiểm chứng UI:** thử ngày không có dữ liệu, lỗi DB, benchmark NULL, search `[`, tên laptop bình thường, và All-Time sau khi đổi grain. Không có giá trị tự dựng hoặc nhãn khác ý nghĩa query.

## 7. Bước 6 — Bổ sung dbt data tests

### 7.1 Test khóa ID ở nguồn và Silver

Trong `dbt/models/model_tests.yml`, thêm cột `id` vào entry `silver_user_event_tracking` hiện có, không tạo model entry trùng tên:

```yaml
  - name: silver_user_event_tracking
    columns:
      - name: id
        tests:
          - unique
          - not_null
      - name: server_timestamp
        tests:
          - not_null
      - name: session_id
        tests:
          - not_null
      - name: event_name
        tests:
          - not_null
```

Trong entry source `user_event_tracking` ở `sources.yml`, thêm `columns`. Có thể đồng thời chuyển freshness sang cấu trúc config hiện đại cho dbt đang dùng:

```yaml
      - name: user_event_tracking
        description: "Clickstream events with JSON payloads"
        config:
          loaded_at_field: "to_timestamp(event_received_on_server_timestamp)"
          freshness:
            warn_after: {count: 24, period: hour}
            error_after: {count: 48, period: hour}
        columns:
          - name: id
            tests: [unique, not_null]
          - name: event_received_on_server_timestamp
            tests: [not_null]
```

Nếu test source fail vì dữ liệu cũ, điều tra/dọn đúng phiên bản trước khi chạy tiếp. `unique_key` của dbt không tự tạo unique constraint trong warehouse.

### 7.2 Đối soát total_events

Tạo `dbt/tests/assert_daily_event_counts.sql`. Singular test pass khi query trả về **0 dòng**:

```sql
WITH expected AS (
    SELECT
        (server_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS event_date,
        COUNT(*) AS total_events
    FROM {{ ref('silver_user_event_tracking') }}
    GROUP BY 1
)
SELECT COALESCE(e.event_date, a.event_date) AS event_date
FROM expected e
FULL OUTER JOIN {{ ref('mart_daily_site_kpis') }} a USING (event_date)
WHERE e.total_events IS DISTINCT FROM a.total_events
```

Tạo `dbt/tests/assert_daily_rates.sql`, chỉ áp dụng sau khi triển khai định nghĩa co-reach mới:

```sql
SELECT *
FROM {{ ref('mart_daily_site_kpis') }}
WHERE total_events < total_sessions
   OR detail_rate_pct NOT BETWEEN 0 AND 100
   OR comparison_rate_pct NOT BETWEEN 0 AND 100
   OR (sessions_with_pageview > 0 AND detail_rate_pct IS NULL)
   OR (sessions_with_detail_view > 0 AND comparison_rate_pct IS NULL)
```

### 7.3 Bắt ID bị bỏ sót giữa raw và Silver

Tạo `dbt/tests/assert_raw_silver_event_ids.sql`:

```sql
WITH expected AS (
    SELECT DISTINCT id
    FROM {{ source('raw', 'user_event_tracking') }}
    WHERE event_name IS NOT NULL AND session_id IS NOT NULL
), actual AS (
    SELECT id FROM {{ ref('silver_user_event_tracking') }}
)
SELECT COALESCE(e.id, a.id) AS id
FROM expected e
FULL OUTER JOIN actual a USING (id)
WHERE e.id IS NULL OR a.id IS NULL
```

Điều kiện eligible cố ý khớp Silver hiện tại. Các row nguồn bị loại vì thiếu event/session vẫn phải được đo riêng, không coi việc Silver `not_null` pass là nguồn sạch. Có thể thêm singular test trả về raw row có các trường bắt buộc NULL để fail sớm; nếu chọn quarantine thay vì fail thì cần bảng rejected rows và số lượng theo run.

ID đầy đủ chưa chứng minh payload đúng phiên bản: cần test đối chiếu timestamp và các payload quan trọng theo ID khi nguồn cho phép update. Test ID trên quét toàn bộ lịch sử; với quy mô hiện tại có thể chấp nhận, khi lớn hơn cần đo chi phí.

## 8. Bước 6 tiếp — Integration tests cho ingestion

Tạo `tests/test_ingestion.py` theo mẫu dưới. Dùng `unittest` có sẵn trong Python nên không cần cài pytest. Test tạo/xóa hai bảng ở database local test; không chạy ingestion toàn project hay gọi ClickHouse.

```python
import os
import unittest

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from ingestion.clickhouse_to_postgres import (
    load_to_postgres,
    load_incremental_to_postgres,
    prepare_event_batch,
)


class BatchTests(unittest.TestCase):
    def test_latest_version_wins_regardless_of_order(self):
        batch = pd.DataFrame([
            {"id": 1, "event_received_on_server_timestamp": 200, "payload": "new"},
            {"id": 1, "event_received_on_server_timestamp": 100, "payload": "old"},
        ])
        for data in (batch, batch.iloc[::-1]):
            result = prepare_event_batch(data)
            self.assertEqual(result.iloc[0]["payload"], "new")

    def test_conflicting_latest_version_fails(self):
        batch = pd.DataFrame([
            {"id": 1, "event_received_on_server_timestamp": 200, "payload": "a"},
            {"id": 1, "event_received_on_server_timestamp": 200, "payload": "b"},
        ])
        with self.assertRaises(ValueError):
            prepare_event_batch(batch)


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Cần PostgreSQL local test")
class PostgresTests(unittest.TestCase):
    def setUp(self):
        url = make_url(os.environ["TEST_DATABASE_URL"])
        if (
            url.host not in {"localhost", "127.0.0.1"}
            or url.port != 55432
            or url.database != "laplaptech_pipeline_test"
        ):
            raise RuntimeError("Test chỉ cho phép instance local port 55432 đã hướng dẫn")
        self.engine = create_engine(url)
        self.addCleanup(self.engine.dispose)
        with self.engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            conn.execute(text("DROP TABLE IF EXISTS raw.brand"))
            conn.execute(text("DROP TABLE IF EXISTS raw.user_event_tracking"))

    def rows(self, table):
        with self.engine.connect() as conn:
            return conn.execute(text(f"SELECT * FROM raw.{table} ORDER BY id")).all()

    def test_full_refresh_rolls_back_failed_insert(self):
        good = pd.DataFrame([{"id": 1, "name": "old"}])
        load_to_postgres(good, "brand", self.engine, truncate_first=True)
        before = self.rows("brand")
        bad = pd.DataFrame([{"id": "not_an_integer", "name": "new"}])
        with self.assertRaises((SQLAlchemyError, pd.errors.DatabaseError)):
            load_to_postgres(bad, "brand", self.engine, truncate_first=True)
        self.assertEqual(self.rows("brand"), before)

    def test_empty_dimension_fails_and_preserves_old_data(self):
        good = pd.DataFrame([{"id": 1, "name": "old"}])
        load_to_postgres(good, "brand", self.engine, truncate_first=True)
        before = self.rows("brand")
        with self.assertRaises(ValueError):
            load_to_postgres(good.iloc[:0], "brand", self.engine, truncate_first=True)
        self.assertEqual(self.rows("brand"), before)

    def test_incremental_replay_and_older_batch(self):
        newest = pd.DataFrame([
            {"id": 1, "event_received_on_server_timestamp": 200, "payload": "new"}
        ])
        old = pd.DataFrame([
            {"id": 1, "event_received_on_server_timestamp": 100, "payload": "old"}
        ])
        load_incremental_to_postgres(newest, "user_event_tracking", self.engine)
        before = self.rows("user_event_tracking")
        load_incremental_to_postgres(newest, "user_event_tracking", self.engine)
        self.assertEqual(self.rows("user_event_tracking"), before)
        load_incremental_to_postgres(old, "user_event_tracking", self.engine)
        self.assertEqual(self.rows("user_event_tracking"), before)
        with self.engine.connect() as conn:
            remaining = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'raw' AND table_name LIKE '_stg_%'
            """)).scalar()
        self.assertEqual(remaining, 0)


if __name__ == "__main__":
    unittest.main()
```

Chạy từ thư mục gốc:

```powershell
python -m unittest discover -s tests -v
```

**Mong đợi:** 5 test pass nếu `TEST_DATABASE_URL` đã được set và DB ready. Không có URL thì 3 integration test bị skip, không được báo cáo là đã kiểm chứng rollback/idempotency trên PostgreSQL.

Thêm các case tiếp theo trước khi đưa vào vận hành: insert staging lỗi không đổi raw; late event nằm trong window được nạp; ngoài window chỉ được khôi phục qua backfill; timezone sát nửa đêm; cùng session qua hai ngày; source có JSON lỗi; hai writer tuân cùng lock. Các test giả lập thứ tự gọi hàm không thay thế được test transaction trên PostgreSQL thật.

## 9. Bước 7 — Freshness, artifact và môi trường tái lập

### 9.1 Chạy freshness rõ ràng và bỏ bước test trùng

Giữ nguyên bước ingestion của workflow sync. Thay hai bước dbt cuối bằng:

```yaml
      - name: Check source freshness
        working-directory: dbt
        run: dbt source freshness --profiles-dir .

      - name: Build models and run data tests
        working-directory: dbt
        run: dbt build --profiles-dir .

      - name: Save dbt run artifacts
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: dbt-results-${{ github.run_id }}-${{ github.run_attempt }}
          path: |
            dbt/target/manifest.json
            dbt/target/run_results.json
            dbt/target/sources.json
          if-no-files-found: warn
          retention-days: 14
```

Với dbt Core 1.12 đang cài trong project, `build` đã chạy data tests nhưng không chạy source freshness; không cần `dbt test` lại toàn bộ ngay sau build. Tài liệu chính thức: [build](https://docs.getdbt.com/reference/commands/build), [source freshness](https://docs.getdbt.com/reference/commands/source). Các trang online có cả phiên bản dbt mới hơn; ví dụ ở đây giữ cú pháp đang dùng trong project.

`warn_after: 24h` cảnh báo; `error_after: 48h` mới là lỗi làm dừng bước theo cấu hình này. MAX timestamp đo độ mới của event, không kiểm tra toàn bộ dữ liệu có đầy đủ. Nếu nguồn là dataset lịch sử cố định, freshness theo hiện tại sẽ fail đúng theo cấu hình; cần ghi nhận đây là batch demo/historical và chọn chính sách phù hợp, không tăng threshold tùy tiện để làm xanh job.

Artifact phục vụ kiểm tra thất bại; không thay thế notification. Bật thông báo workflow failure trong GitHub theo nhu cầu. Khi sau này thêm webhook, tránh đưa credential hoặc payload người dùng vào thông báo.

### 9.2 Khóa dependency

Giữ `requirements.txt` để mô tả các dependency trực tiếp. Tạo lock từ môi trường đã kiểm tra chạy tốt:

```powershell
python -m pip check
python -m pip freeze | Out-File -FilePath requirements.lock.txt -Encoding utf8
```

Đọc lại lock, bỏ đường dẫn local/editable hoặc dependency không liên quan trước khi commit. Đây là bản khóa thực dụng cho project học tập, chưa phải lock đa nền tảng có hash.

Trong cả workflow sync và validate, cài `pip install -r requirements.lock.txt`. Kiểm tra lại cài đặt trên Linux runner vì venv hiện tại được tạo trên Windows; nếu freeze có package chỉ dành cho Windows thì dùng environment marker hoặc tạo lock trên Linux. Cố định Python version giữa local và CI, cập nhật dependency có chủ đích rồi chạy lại kiểm tra.

### 9.3 Chạy một lượt end-to-end trong dev

Sau khi đã có raw dev và cấu hình đúng, chạy từ root:

```powershell
python ingestion/clickhouse_to_postgres.py
python -m dotenv -f .env run -- dbt source freshness --project-dir dbt --profiles-dir dbt
python -m dotenv -f .env run -- dbt build --project-dir dbt --profiles-dir dbt
python -m unittest discover -s tests -v
```

Lệnh ingestion tự đọc `.env`, nhưng các biến môi trường shell đã tồn tại có thể được ưu tiên hơn `.env`; kiểm tra đúng DB trước khi chạy. Không in password ra terminal.

Khi cần cập nhật toàn bộ lịch sử model event sau thay đổi parsing, cân nhắc chạy trong dev:

```powershell
python -m dotenv -f .env run -- dbt build --full-refresh --select silver_user_event_tracking+ --project-dir dbt --profiles-dir dbt
```

Lệnh này rebuild incremental table và downstream được chọn, có ghi/thay dữ liệu. Đo thời gian và dung lượng trước khi áp dụng lên warehouse chính. Không dùng full-refresh để chữa dữ liệu chưa có trong raw.

## 10. Bước 8 — Cập nhật tài liệu và tiêu chí nghiệm thu

Sửa README và `docs/architecture/upgrade_plan.md` sau khi đã có kết quả thật:

- Thay “56/56 tests” bằng số **model / data test / exposure** riêng. Artifact cũ có 29 model success, 26 test pass, 1 exposure no-op; exposure không phải test. Sau khi thêm test, lấy số mới từ manifest/run results.
- Ghi chính xác transaction atomic ở mức bảng, không tuyên bố toàn pipeline atomic.
- Mô tả giới hạn lookback, cách backfill và loại timestamp dùng làm watermark.
- Ghi grain của mỗi mart: event, session, session-day, laptop hay brand pair.
- Bỏ các khẳng định “không bao giờ bỏ sót”, “idempotent 100%”, “Enterprise Production-Grade” khi chưa có bằng chứng và giới hạn rõ ràng.
- Ghi thời gian chạy bằng benchmark có số row, môi trường và ngày đo; không coi khoảng 13 giây là bảo đảm cho mọi lần chạy.

Checklist hoàn thành:

- [ ] Full refresh lỗi insert giữ lại dữ liệu cũ.
- [ ] Batch lặp không tăng số ID; batch cũ không ghi đè bản mới.
- [ ] Batch cùng ID/timestamp nhưng payload xung đột bị phát hiện.
- [ ] Không còn staging dùng tên cố định giữa các run.
- [ ] Warehouse chính chỉ có một orchestrator active và không chạy chồng các lượt sync.
- [ ] Raw/Silver có test ID; daily event count đối soát pass.
- [ ] Dashboard thể hiện đúng ngày, grain, N/A và lỗi truy vấn.
- [ ] Freshness thực sự được chạy; artifacts được giữ khi job lỗi.
- [ ] Có kết quả integration test trên PostgreSQL test, không chỉ parse pass.
- [ ] README phản ánh đúng việc đã làm và các giới hạn còn lại.

## 11. Có nên thay GitHub Actions bằng Airflow?

### 11.1 Khuyến nghị cho project này

**Chưa thay job đang vận hành. Sau khi sửa xong, làm Airflow lab nếu mục tiêu là học Data Engineering hoặc bổ sung portfolio.** Đây là khuyến nghị dựa trên độ phức tạp của repo hiện tại, không có ngưỡng số dòng bắt buộc phải chuyển sang Airflow.

| Nhu cầu | Đề xuất |
|---|---|
| Một pipeline ingestion → freshness → dbt, hai lần/ngày | GitHub Actions đủ dùng |
| Muốn học DAG, retry từng task và quan sát run trong UI | Airflow lab có giá trị |
| Nhiều nguồn với dependency, cần rerun từng phần và quản lý lịch sử task | Cân nhắc Airflow cho vận hành |
| Backfill theo ngày/giờ | Chuẩn bị extraction theo khoảng trước, rồi dùng Airflow điều phối |
| Không muốn quản lý thêm dịch vụ chạy thường trực | Giữ Actions và cải thiện logging/tests |

Airflow biểu diễn workflow bằng DAG/task và cần thêm các thành phần vận hành như scheduler, DAG processor, API server và metadata DB. Khả năng này hữu ích khi dependency phức tạp, nhưng tạo thêm công việc quản lý cho project hiện tại. Xem [Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html).

Trên Windows, dùng WSL2 hoặc Linux containers; tài liệu Airflow khuyến nghị tối thiểu 4 GB RAM và hỗ trợ production trên Linux. Tài nguyên thực tế còn phụ thuộc deployment. Không thêm Airflow trực tiếp vào venv pipeline hiện tại; dùng môi trường/image riêng. Xem [Prerequisites](https://airflow.apache.org/docs/apache-airflow/stable/installation/prerequisites.html).

### 11.2 Lộ trình Airflow lab

1. Hoàn thành các fix và test trước.
2. Tạo `orchestration-lab/` riêng. Nếu theo quickstart có file `.env`, đặt file đó ở thư mục lab, giữ `.env` pipeline ở root.
3. Chọn một bản Airflow 3.x được hỗ trợ, dùng hướng dẫn cài đặt/constraints đúng phiên bản. [Docker Compose quickstart](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) dùng để học và thử, không mặc định là cấu hình production.
4. Dùng PostgreSQL metadata riêng cho Airflow; không trộn metadata vào schema raw/gold hoặc ngân sách warehouse nhỏ hiện tại.
5. Trong worker Linux, mount/copy repo vào `/opt/airflow/project`, tạo runtime riêng `/opt/pipeline-venv` và cài dependencies lock của pipeline vào đó. Cài provider standard phù hợp trong môi trường Airflow. Không cài package lúc import DAG hoặc mỗi lần task chạy.
6. Inject các biến ClickHouse/PostgreSQL **dev** vào worker qua cơ chế secret/env của deployment. Không đưa password vào DAG, không truyền DataFrame qua XCom.
7. Chạy DAG manual trước; kiểm tra failed task, retry và dependency bằng UI.
8. Chỉ chuyển lịch chính sau khi lab chạy ổn và đã thống nhất một orchestrator duy nhất có quyền ghi warehouse chính.

### 11.3 DAG mẫu cho Airflow 3.x — chỉ chạy manual trong lab

Ví dụ đặt trong thư mục DAG của môi trường lab thành `laplaptech_lab.py`. Mẫu yêu cầu worker có repo/runtime/env như bước trên; không thể chỉ copy DAG vào Airflow trống rồi chạy.

```python
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="laplaptech_lab",
    start_date=pendulum.datetime(2026, 10, 19, tz="Asia/Ho_Chi_Minh"),
    schedule=None,  # Manual để thử, chưa thay lịch Actions hiện tại.
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=25),
    },
    tags=["laplaptech", "lab"],
) as dag:
    ingest = BashOperator(
        task_id="ingest_raw",
        cwd="/opt/airflow/project",
        bash_command="set -euo pipefail\n/opt/pipeline-venv/bin/python ingestion/clickhouse_to_postgres.py",
        append_env=True,
        do_xcom_push=False,
    )
    freshness = BashOperator(
        task_id="check_freshness",
        cwd="/opt/airflow/project",
        bash_command="set -euo pipefail\n/opt/pipeline-venv/bin/dbt source freshness --project-dir dbt --profiles-dir dbt",
        append_env=True,
        do_xcom_push=False,
    )
    build = BashOperator(
        task_id="build_and_test",
        cwd="/opt/airflow/project",
        bash_command="set -euo pipefail\n/opt/pipeline-venv/bin/dbt build --project-dir dbt --profiles-dir dbt",
        append_env=True,
        do_xcom_push=False,
    )

    ingest >> freshness >> build
```

Import/operator tham khảo [BashOperator chính thức](https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/bash.html). Snippet được kiểm tra cú pháp Python; chưa được import hoặc thực thi trong Airflow runtime của project.

`max_active_runs=1` chặn các run của DAG này chạy chồng, không chặn Actions, laptop hay DAG khác. Retry task chỉ an toàn khi code nạp dữ liệu đã idempotent. Airflow cũng không tự làm transaction, dedup hay sửa KPI.

### 11.4 Điều kiện chuyển lịch chính và rollback

1. So sánh kết quả Actions và Airflow trên hai database dev riêng hoặc chạy lần lượt; không chạy hai writer song song vào DB chính.
2. Kiểm chứng retry, thất bại freshness, rerun dbt và lưu artifact/log trong Airflow. DAG mẫu chưa làm bước publish artifact, notification hay audit run.
3. Khi chuyển, dừng mọi run cũ; gỡ `schedule` và bước sync trên push của Actions (giữ workflow validate). Sau đó mới bật schedule Airflow.
4. Giữ đúng hai giờ 08:15 và 16:45 Việt Nam bằng timetable phù hợp phiên bản hoặc thiết kế trigger tương ứng; một cron đơn không biểu diễn trực tiếp hai tổ hợp phút/giờ bất quy tắc này.
5. Chỉ bật catchup/backfill sau khi task dùng data interval thật. Script đã có tham số khoảng, nhưng DAG mẫu trên chưa truyền `--start`/`--end`; khi đó script vẫn dùng MAX đích và chạy logical date cũ chưa phải backfill lịch sử.
6. Nếu rollback orchestrator: pause DAG, chờ/dừng run đang ghi, rồi mới khôi phục lịch Actions. Không chạy cả hai lịch cùng lúc.

**Quyết định thực dụng:** sửa correctness trước, giữ Actions cho pipeline chính; thêm Airflow lab sau để học orchestration với các tình huống lỗi đã có test. Chỉ thay orchestrator khi lợi ích rerun/dependency/backfill vượt phần vận hành phải thêm.

## 12. Phạm vi đã kiểm tra khi viết hướng dẫn

Các kiểm tra snippet dưới đây ghi nhận lúc soạn mẫu. Source sau sửa đã chạy 21/21 Python test, gồm 9 PostgreSQL integration test; xem [kết quả hiện tại](ingestion_operations.md) để không nhầm với bộ mẫu 5 test trong tài liệu.

- Đã đối chiếu snippet với tên hàm, bảng và workflow hiện có.
- Đã kiểm tra cú pháp 13 snippet Python và parse 6 snippet YAML trong tài liệu.
- Đã chạy 2 test dedup offline trong tài liệu: chọn bản mới nhất không phụ thuộc thứ tự, và phát hiện payload xung đột cùng timestamp; cả hai pass.
- Đã đưa daily KPI, 3 singular test, source config và model test mẫu vào một bản sao project dưới `scratch/`; `dbt parse` thành công với dbt Core 1.12.3 / dbt-postgres 1.11.0.
- Không coi `dbt parse` là bằng chứng SQL/data tests thành công.
- Chưa chạy các bản sửa trên ClickHouse/Neon chính; chưa triển khai Airflow hay đổi lịch đang vận hành.
- Kết quả integration/end-to-end chỉ được ghi PASS sau khi thực sự chạy trên database test/dev.
