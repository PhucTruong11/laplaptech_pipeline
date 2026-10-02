"""
LaplapTech Pipeline — ClickHouse to PostgreSQL Ingestion
=========================================================
Script chịu trách nhiệm trích xuất dữ liệu (Extract) từ nguồn ClickHouse 
và tải (Load) nguyên bản vào schema `raw` của Data Warehouse PostgreSQL (Local / Supabase).

Chiến lược nạp dữ liệu:
- Full Refresh (TRUNCATE + Append): Dành cho các bảng danh mục (dimension) và bảng tra cứu nhỏ.
- Incremental Load (Watermark + Lookback Window): Dành cho bảng sự kiện lớn (user_event_tracking).

Cách chạy thủ công:
    python ingestion/clickhouse_to_postgres.py
"""

import os
import sys
import logging
from datetime import datetime, timedelta

import clickhouse_connect
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Cấu hình & Biến toàn cục (Configuration)
# ---------------------------------------------------------------------------

# Tải các biến môi trường từ file .env
load_dotenv()

# Cấu hình định dạng logging chuẩn
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Danh sách bảng kích thước nhỏ / bảng danh mục cần làm mới toàn bộ mỗi lần chạy (Full Refresh)
FULL_REFRESH_TABLES = [
    "brand",
    "cpu_model",
    "gpu_model",
    "laptop_model",
    "laptop_benchmark_result",
]

# Danh sách bảng sự kiện lớn cần nạp gia tăng (Incremental) kèm tên cột mốc thời gian (Watermark)
INCREMENTAL_TABLES = {
    "user_event_tracking": "event_received_on_server_timestamp",
}

# Tên schema đích trong PostgreSQL để chứa dữ liệu thô ban đầu
RAW_SCHEMA = "raw"

# ---------------------------------------------------------------------------
# Các hàm tiện ích & Khởi tạo kết nối (Helpers & Connections)
# ---------------------------------------------------------------------------


def require_env(name: str) -> str:
    """
    Lấy giá trị của một biến môi trường bắt buộc.
    Nếu biến chưa được thiết lập, ném ra ngoại lệ RuntimeError để dừng pipeline ngay lập tức.
    """
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường bắt buộc: {name}")
    return value


def get_clickhouse_client():
    """
    Khởi tạo ClickHouse client kết nối tới máy chủ nguồn.
    Lưu ý: Cổng 80 chạy giao thức HTTP thuần nên secure=False.
    """
    return clickhouse_connect.get_client(
        host=require_env("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
        username=require_env("CLICKHOUSE_USER"),
        password=require_env("CLICKHOUSE_PASSWORD"),
        database=os.getenv("CLICKHOUSE_DATABASE", "laplaptech"),
        secure=False,  # Cổng 80 là giao thức HTTP thông thường
    )


def get_postgres_engine():
    """
    Khởi tạo engine SQLAlchemy kết nối đến cơ sở dữ liệu PostgreSQL đích.
    Sử dụng driver psycopg2 chuẩn cho hiệu năng cao.
    """
    user = require_env("POSTGRES_USER")
    password = require_env("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DATABASE", "laplaptech_pipeline")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


def ensure_database_exists():
    """
    Kiểm tra xem Database đích trong PostgreSQL đã tồn tại chưa.
    Nếu chưa, kết nối tạm vào database mặc định 'postgres' với quyền AUTOCOMMIT để tạo mới database.
    """
    user = require_env("POSTGRES_USER")
    password = require_env("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DATABASE", "laplaptech_pipeline")

    # Kết nối vào DB 'postgres' mặc định để kiểm tra và cấp phát DB mới nếu cần
    admin_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

    with admin_engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db"),
            {"db": database},
        )
        if not result.fetchone():
            conn.execute(text(f'CREATE DATABASE "{database}"'))
            logger.info(f"Đã khởi tạo cơ sở dữ liệu mới: {database}")
        else:
            logger.info(f"Cơ sở dữ liệu đã tồn tại sẵn: {database}")

    admin_engine.dispose()


def ensure_schema_exists(engine):
    """
    Đảm bảo schema chứa dữ liệu thô (raw) đã tồn tại trong PostgreSQL.
    Tạo mới nếu chưa có bằng lệnh CREATE SCHEMA IF NOT EXISTS.
    """
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))
        conn.commit()
    logger.info(f"Schema '{RAW_SCHEMA}' đã sẵn sàng.")


# ---------------------------------------------------------------------------
# Trích xuất và Nạp dữ liệu (Extraction & Loading)
# ---------------------------------------------------------------------------


def extract_full_table(ch_client, table_name: str) -> pd.DataFrame:
    """
    Trích xuất toàn bộ dữ liệu từ 1 bảng trong ClickHouse.
    Thường áp dụng cho các bảng danh mục nhỏ như brand, cpu_model, gpu_model...
    """
    query = f"SELECT * FROM {table_name}"
    logger.info(f"Đang trích xuất toàn bộ bảng: {table_name}")
    result = ch_client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    logger.info(f"  → Trích xuất thành công {len(df):,} dòng.")
    return df


def extract_incremental(
    ch_client, table_name: str, timestamp_col: str, engine
) -> pd.DataFrame:
    """
    Trích xuất dữ liệu gia tăng (Incremental) từ ClickHouse dựa vào Watermark kết hợp Lookback Window.

    Quy trình:
    1. Truy vấn PostgreSQL để tìm mốc thời gian lớn nhất (MAX timestamp) hiện có trong bảng raw.
    2. Nếu đã có dữ liệu cũ:
       - Trừ lùi 1 giờ (Lookback Window = 1 hour / 3600 seconds) để thu thập cả 'Late-arriving data' 
         (dữ liệu ghi chậm do lag mạng, retry).
       - Xử lý linh hoạt cả kiểu int/float (epoch timestamp) và kiểu chuỗi/datetime.
    3. Nếu chưa có dữ liệu (lần chạy đầu tiên): Kéo toàn bộ bảng.
    4. Dữ liệu trùng lặp do cửa sổ lookback sinh ra sẽ được tầng dbt Silver khử trùng (Deduplication).
    """
    max_ts = None
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    f'SELECT MAX("{timestamp_col}") FROM {RAW_SCHEMA}.{table_name}'
                )
            )
            row = result.fetchone()
            if row and row[0]:
                max_ts = row[0]
    except Exception:
        # Nếu bảng chưa tồn tại trong PostgreSQL thì sẽ thực hiện full load ban đầu
        pass

    if max_ts:
        # Trường hợp timestamp dạng Epoch (số nguyên / giây): lùi 3600 giây (1 giờ)
        if isinstance(max_ts, (int, float)):
            lookback_ts = int(max_ts) - 3600
            query_val = f"{lookback_ts}"
        else:
            # Trường hợp timestamp dạng chuỗi hoặc datetime: lùi 1 giờ bằng timedelta
            if isinstance(max_ts, str):
                max_ts_dt = pd.to_datetime(max_ts)
            else:
                max_ts_dt = max_ts
            lookback_ts = max_ts_dt - timedelta(hours=1)
            query_val = f"'{lookback_ts.strftime('%Y-%m-%d %H:%M:%S')}'"

        # Lấy các bản ghi từ mốc lookback_ts trở đi
        query = f"SELECT * FROM {table_name} WHERE {timestamp_col} >= {query_val}"
        logger.info(
            f"Trích xuất Incremental: {table_name} (Lookback Window từ {lookback_ts})"
        )
    else:
        query = f"SELECT * FROM {table_name}"
        logger.info(
            f"Trích xuất Full lần đầu: {table_name}"
        )

    result = ch_client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    logger.info(f"  → Trích xuất được {len(df):,} dòng.")
    return df


def load_to_postgres(
    df: pd.DataFrame, table_name: str, engine, if_exists: str = "append", truncate_first: bool = False
):
    """
    Tải DataFrame vào bảng tương ứng trong schema 'raw' của PostgreSQL.

    Điểm kiến trúc quan trọng:
    - truncate_first=True: Áp dụng cho các bảng Dimension (Full Refresh).
      Dùng `TRUNCATE TABLE raw.<table_name>` thay vì `DROP TABLE ... CASCADE` hoặc `if_exists='replace'`.
      Lý do: Lệnh DROP sẽ vô tình xóa luôn các VIEW phụ thuộc ở tầng Bronze trong PostgreSQL mà dbt đã tạo,
      khiến dbt bị gãy (relation does not exist). TRUNCATE giữ nguyên cấu trúc bảng và các view phụ thuộc!
    - if_exists="append": Ghi tiếp dữ liệu vào bảng sau khi đã dọn sạch (hoặc nạp nối tiếp cho Incremental).
    - chunksize=10000 & method="multi": Bulk insert theo từng lô 10k bản ghi để tối ưu tốc độ và bộ nhớ RAM.
    """
    if df.empty:
        logger.info(f"  → 0 dòng mới cần nạp cho bảng {table_name}")
        return

    logger.info(f"Đang nạp {len(df):,} dòng vào {RAW_SCHEMA}.{table_name}...")

    try:
        if truncate_first:
            with engine.begin() as conn:
                # Kiểm tra bảng có tồn tại chưa trước khi TRUNCATE
                # Lần chạy đầu tiên (fresh database) sẽ bỏ qua TRUNCATE vì bảng chưa tồn tại
                table_exists = conn.execute(
                    text(
                        f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = '{RAW_SCHEMA}'
                            AND table_name = '{table_name}'
                        )
                        """
                    )
                ).scalar()

                if table_exists:
                    # TRUNCATE giữ nguyên view Bronze, đảm bảo tính ổn định (Idempotency)
                    conn.execute(text(f"TRUNCATE TABLE {RAW_SCHEMA}.{table_name}"))
                    logger.info(f"  → Đã dọn sạch bảng (TRUNCATE): {table_name}")
                else:
                    logger.info(f"  → Bảng chưa tồn tại, bỏ qua TRUNCATE (lần chạy đầu tiên): {table_name}")

        # Nạp dữ liệu vào bảng bằng cơ chế bulk insert
        df.to_sql(
            table_name,
            con=engine,
            schema=RAW_SCHEMA,
            if_exists="append",
            index=False,
            chunksize=10000,
            method="multi",
        )
        logger.info(f"  → Nạp thành công {len(df):,} dòng vào PostgreSQL ({table_name})")
    except Exception as e:
        logger.error(f"Lỗi khi nạp bảng {table_name}: {e}")
        raise


# ---------------------------------------------------------------------------
# Luồng thực thi chính (Main Pipeline)
# ---------------------------------------------------------------------------


def run_pipeline():
    """
    Điều phối toàn bộ luồng Ingestion (ClickHouse → PostgreSQL):
    Bước 1: Khởi tạo database và schema 'raw'.
    Bước 2: Kết nối ClickHouse.
    Bước 3: Nạp Full Refresh các bảng danh mục (TRUNCATE + Append).
    Bước 4: Nạp Incremental bảng sự kiện clickstream (Lookback Window).
    Bước 5: Thống kê số lượng dòng thực tế sau nạp.
    """
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("LaplapTech Pipeline — Bắt đầu quá trình Ingestion")
    logger.info("=" * 60)

    # 1. Kiểm tra & đảm bảo Database và Schema đã sẵn sàng
    ensure_database_exists()
    engine = get_postgres_engine()
    ensure_schema_exists(engine)

    # 2. Khởi tạo kết nối ClickHouse
    ch_client = get_clickhouse_client()
    logger.info(f"Đã kết nối thành công tới ClickHouse: {os.getenv('CLICKHOUSE_HOST')}")

    # 3. Nạp các bảng Full Refresh (Dimension tables)
    for table_name in FULL_REFRESH_TABLES:
        try:
            df = extract_full_table(ch_client, table_name)
            load_to_postgres(df, table_name, engine, truncate_first=True)
        except Exception as e:
            logger.error(f"Lỗi khi xử lý bảng {table_name}: {e}")
            raise

    # 4. Nạp bảng Incremental (Fact tables — Deduplication sẽ do dbt phụ trách)
    for table_name, ts_col in INCREMENTAL_TABLES.items():
        try:
            df = extract_incremental(ch_client, table_name, ts_col, engine)
            load_to_postgres(df, table_name, engine, truncate_first=False)
        except Exception as e:
            logger.error(f"Lỗi khi xử lý bảng {table_name}: {e}")
            raise

    # 5. Tổng kết thời gian chạy & đếm số lượng dòng trong kho
    elapsed = datetime.now() - start
    logger.info("=" * 60)
    logger.info(f"Ingestion hoàn tất thành công trong {elapsed}")

    with engine.connect() as conn:
        for table in FULL_REFRESH_TABLES + list(INCREMENTAL_TABLES.keys()):
            try:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table}")
                )
                count = result.fetchone()[0]
                logger.info(f"  {table}: {count:,} dòng")
            except Exception:
                logger.info(f"  {table}: (không tìm thấy bảng)")

    logger.info("=" * 60)
    engine.dispose()


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Pipeline thất bại: {e}")
        sys.exit(1)
