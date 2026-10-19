"""Nạp ClickHouse vào PostgreSQL raw, atomic theo mỗi bảng.

Dimension dùng full refresh; event dùng watermark/lookback hoặc khoảng backfill.
Database managed như Neon phải được cấp sẵn, trừ khi bật provisioning rõ ràng.
"""

import argparse
import json
import logging
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

FULL_REFRESH_TABLES = [
    "brand", "cpu_model", "gpu_model", "laptop_model", "laptop_benchmark_result"
]
INCREMENTAL_TABLES = {"user_event_tracking": "event_received_on_server_timestamp"}
RAW_SCHEMA = "raw"


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường bắt buộc: {name}")
    return value


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    if value.lower() not in {"true", "false", "1", "0"}:
        raise ValueError(f"{name} phải là true/false hoặc 1/0")
    return value.lower() in {"true", "1"}


def get_clickhouse_client():
    port = int(os.getenv("CLICKHOUSE_PORT") or "80")
    return clickhouse_connect.get_client(
        host=require_env("CLICKHOUSE_HOST"), port=port,
        username=require_env("CLICKHOUSE_USER"),
        password=require_env("CLICKHOUSE_PASSWORD"),
        database=os.getenv("CLICKHOUSE_DATABASE") or "laplaptech",
        secure=env_bool("CLICKHOUSE_SECURE", default=port in {443, 8443}),
    )


def postgres_url(database: str | None = None) -> URL:
    """URL.create giữ nguyên password có @, /, : mà không cần escape thủ công."""
    return URL.create(
        "postgresql+psycopg2",
        username=require_env("POSTGRES_USER"),
        password=require_env("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST") or "localhost",
        port=int(os.getenv("POSTGRES_PORT") or "5432"),
        database=database or os.getenv("POSTGRES_DATABASE") or "laplaptech_pipeline",
        query={"sslmode": os.getenv("POSTGRES_SSLMODE") or "prefer"},
    )


def get_postgres_engine():
    return create_engine(postgres_url(), pool_pre_ping=True)


def quote_ident(conn, name: str) -> str:
    return conn.dialect.identifier_preparer.quote_identifier(name)


def raw_relation(conn, table_name: str) -> str:
    return f"{quote_ident(conn, RAW_SCHEMA)}.{quote_ident(conn, table_name)}"


def ensure_database_exists():
    """Chỉ tạo DB khi POSTGRES_CREATE_DATABASE=true; Neon không cần bước này."""
    if not env_bool("POSTGRES_CREATE_DATABASE"):
        return
    database = postgres_url().database
    admin_engine = create_engine(postgres_url("postgres"), isolation_level="AUTOCOMMIT")
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"), {"db": database}
            ).scalar()
            if not exists:
                conn.execute(text(f"CREATE DATABASE {quote_ident(conn, database)}"))
    finally:
        admin_engine.dispose()


def ensure_schema_exists(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(conn, RAW_SCHEMA)}"))


def lock_raw_table(conn, table_name: str):
    # Transaction lock tương thích pooler; không giữ session lock.
    conn.execute(text("SET LOCAL lock_timeout = '30s'"))
    conn.execute(
        text("SELECT pg_advisory_xact_lock(74621, hashtext(:resource))"),
        {"resource": f"{RAW_SCHEMA}.{table_name}"},
    )


def assert_same_columns(conn, table_name: str, df: pd.DataFrame):
    existing = {c["name"] for c in inspect(conn).get_columns(table_name, schema=RAW_SCHEMA)}
    incoming = set(df.columns)
    if existing != incoming:
        raise ValueError(
            f"Schema thay đổi ở {table_name}: "
            f"thiếu={sorted(existing - incoming)}, thừa={sorted(incoming - existing)}"
        )


def write_frame(df: pd.DataFrame, table_name: str, conn):
    if not df.columns.is_unique or len(df.columns) == 0:
        raise ValueError("Batch phải có tên cột duy nhất và ít nhất một cột")
    df.to_sql(
        table_name, con=conn, schema=RAW_SCHEMA, if_exists="append", index=False,
        chunksize=max(1, min(1000, 60000 // len(df.columns))), method="multi",
    )


def prepare_event_batch(
    df: pd.DataFrame,
    primary_key: str = "id",
    timestamp_col: str = "event_received_on_server_timestamp",
) -> pd.DataFrame:
    """Giữ version mới nhất mỗi ID; xung đột version bằng nhau phải được điều tra."""
    if df.empty:
        return df.copy()
    if not df.columns.is_unique:
        raise ValueError("Batch có tên cột trùng nhau")
    if not {primary_key, timestamp_col}.issubset(df.columns):
        raise ValueError("Batch thiếu ID hoặc watermark")
    batch = df.copy()
    if batch[primary_key].isna().any() or batch[primary_key].astype("string").str.strip().eq("").any():
        raise ValueError("Event thiếu ID")
    batch[timestamp_col] = pd.to_numeric(batch[timestamp_col], errors="raise")
    if not batch[timestamp_col].map(math.isfinite).all():
        raise ValueError("Watermark phải là epoch giây hữu hạn, không được NULL")
    for column in ("event_data", "device"):
        if column in batch:
            batch[column] = batch[column].map(
                lambda value: json.dumps(value, sort_keys=True, ensure_ascii=False)
                if isinstance(value, (dict, list)) else value
            )
    latest = batch.groupby(primary_key)[timestamp_col].transform("max")
    candidates = batch.loc[batch[timestamp_col].eq(latest)].drop_duplicates()
    if candidates.duplicated(primary_key).any():
        raise ValueError("Cùng ID/watermark mới nhất nhưng payload khác nhau; cần version từ nguồn")
    return candidates.reset_index(drop=True)


def extract_full_table(ch_client, table_name: str) -> pd.DataFrame:
    if table_name not in FULL_REFRESH_TABLES:
        raise ValueError(f"Không phải bảng full refresh: {table_name}")
    result = ch_client.query(f"SELECT * FROM {table_name}")
    frame = pd.DataFrame(result.result_rows, columns=result.column_names)
    logger.info("Extract %s: %s dòng", table_name, len(frame))
    return frame


def validate_range(start_epoch, end_epoch):
    if (start_epoch is None) != (end_epoch is None):
        raise ValueError("Backfill cần cả start và end")
    if start_epoch is not None:
        if not all(math.isfinite(v) for v in (start_epoch, end_epoch)) or start_epoch >= end_epoch:
            raise ValueError("Backfill cần start < end và epoch hữu hạn")


def extract_incremental(
    ch_client, table_name: str, timestamp_col: str, engine,
    *, start_epoch: float | None = None, end_epoch: float | None = None,
) -> pd.DataFrame:
    if INCREMENTAL_TABLES.get(table_name) != timestamp_col:
        raise ValueError("Bảng hoặc watermark không thuộc cấu hình incremental")
    validate_range(start_epoch, end_epoch)
    query = f"SELECT * FROM {table_name}"
    parameters = {}
    if start_epoch is not None:
        query += f" WHERE {timestamp_col} >= {{start:Float64}} AND {timestamp_col} < {{end:Float64}}"
        parameters = {"start": start_epoch, "end": end_epoch}
    else:
        max_ts = None
        with engine.connect() as conn:
            if inspect(conn).has_table(table_name, schema=RAW_SCHEMA):
                max_ts = conn.execute(text(
                    f"SELECT MAX({quote_ident(conn, timestamp_col)}) FROM {raw_relation(conn, table_name)}"
                )).scalar()
        if max_ts is not None:
            max_epoch = float(max_ts)
            lookback = int(os.getenv("INGESTION_LOOKBACK_SECONDS") or "7200")
            if not math.isfinite(max_epoch) or lookback < 0:
                raise ValueError("Watermark không hữu hạn hoặc lookback âm")
            query += f" WHERE {timestamp_col} >= {{watermark:Float64}}"
            parameters = {"watermark": max_epoch - lookback}
    # Lỗi quyền/kết nối khi đọc MAX phải fail, không âm thầm full extraction.
    result = ch_client.query(query, parameters=parameters)
    frame = pd.DataFrame(result.result_rows, columns=result.column_names)
    logger.info("Extract %s: %s dòng (backfill=%s)", table_name, len(frame), start_epoch is not None)
    return frame


def load_to_postgres(
    df: pd.DataFrame, table_name: str, engine,
    if_exists: str = "append", truncate_first: bool = False,
):
    if table_name not in FULL_REFRESH_TABLES or if_exists != "append":
        raise ValueError("Loader dimension chỉ hỗ trợ bảng cấu hình sẵn và append")
    if df.empty:
        raise ValueError(f"Nguồn {table_name} rỗng; giữ nguyên dữ liệu cũ")
    with engine.begin() as conn:
        lock_raw_table(conn, table_name)
        exists = inspect(conn).has_table(table_name, schema=RAW_SCHEMA)
        if exists:
            assert_same_columns(conn, table_name, df)
        if exists and truncate_first:
            conn.execute(text(f"TRUNCATE TABLE {raw_relation(conn, table_name)}"))
        write_frame(df, table_name, conn)
    logger.info("Commit %s: %s dòng", table_name, len(df))


def load_incremental_to_postgres(
    df: pd.DataFrame, table_name: str, engine, primary_key: str = "id"
):
    if table_name not in INCREMENTAL_TABLES or primary_key != "id":
        raise ValueError("Loader event chỉ hỗ trợ bảng cấu hình sẵn với khóa id")
    ts_col = INCREMENTAL_TABLES[table_name]
    batch = prepare_event_batch(df, primary_key, ts_col)
    if batch.empty:
        logger.info("Không có event cần nạp: %s", table_name)
        return
    with engine.begin() as conn:
        lock_raw_table(conn, table_name)
        if not inspect(conn).has_table(table_name, schema=RAW_SCHEMA):
            write_frame(batch, table_name, conn)
        else:
            assert_same_columns(conn, table_name, batch)
            target = raw_relation(conn, table_name)
            stg_name = f"_stg_{table_name}_{uuid4().hex[:12]}"
            staging = raw_relation(conn, stg_name)
            pk, ts = quote_ident(conn, primary_key), quote_ident(conn, ts_col)
            columns = ", ".join(quote_ident(conn, c) for c in batch.columns)
            conn.execute(text(f"CREATE TABLE {staging} (LIKE {target})"))
            write_frame(batch, stg_name, conn)
            conn.execute(text(f"""
                DELETE FROM {staging} s USING {target} t
                WHERE s.{pk} = t.{pk} AND s.{ts} < t.{ts}
            """))
            staged_row = ", ".join(f"s.{quote_ident(conn, c)}" for c in batch.columns)
            target_row = ", ".join(f"t.{quote_ident(conn, c)}" for c in batch.columns)
            conflict = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1 FROM {staging} s JOIN {target} t ON s.{pk} = t.{pk}
                    WHERE s.{ts} = t.{ts}
                      AND ROW({staged_row}) IS DISTINCT FROM ROW({target_row})
                )
            """)).scalar()
            if conflict:
                raise ValueError("Raw và batch có cùng ID/watermark nhưng payload khác nhau")
            conn.execute(text(f"DELETE FROM {target} t USING {staging} s WHERE t.{pk} = s.{pk}"))
            conn.execute(text(f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {staging}"))
            conn.execute(text(f"DROP TABLE {staging}"))
    logger.info("Commit batch %s: %s ID đầu vào", table_name, len(batch))


def run_pipeline(*, start_epoch: float | None = None, end_epoch: float | None = None):
    validate_range(start_epoch, end_epoch)
    started = datetime.now(timezone.utc)
    ensure_database_exists()
    engine = get_postgres_engine()
    ch_client = None
    try:
        ensure_schema_exists(engine)
        ch_client = get_clickhouse_client()
        for table_name in FULL_REFRESH_TABLES:
            frame = extract_full_table(ch_client, table_name)
            load_to_postgres(frame, table_name, engine, truncate_first=True)
        for table_name, timestamp_col in INCREMENTAL_TABLES.items():
            frame = extract_incremental(
                ch_client, table_name, timestamp_col, engine,
                start_epoch=start_epoch, end_epoch=end_epoch,
            )
            load_incremental_to_postgres(frame, table_name, engine)
        with engine.connect() as conn:
            for table in FULL_REFRESH_TABLES + list(INCREMENTAL_TABLES):
                if inspect(conn).has_table(table, schema=RAW_SCHEMA):
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {raw_relation(conn, table)}")).scalar()
                    logger.info("%s: %s dòng", table, count)
        logger.info("Ingestion hoàn tất trong %s", datetime.now(timezone.utc) - started)
    finally:
        if ch_client is not None:
            try:
                ch_client.close()
            except Exception:
                logger.warning("Không đóng được ClickHouse client")
        engine.dispose()


def parse_epoch(value: str) -> float:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("thiếu timezone")
        return parsed.timestamp()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Dùng ISO-8601 có timezone, ví dụ 2026-10-01T00:00:00+07:00"
        ) from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=parse_epoch, help="Đầu khoảng backfill (bao gồm)")
    parser.add_argument("--end", type=parse_epoch, help="Cuối khoảng backfill (không bao gồm)")
    args = parser.parse_args()
    try:
        run_pipeline(start_epoch=args.start, end_epoch=args.end)
    except Exception as exc:
        logger.error("Pipeline thất bại: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
