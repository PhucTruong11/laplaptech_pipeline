"""
LaplapTech Pipeline — ClickHouse to PostgreSQL Ingestion
=========================================================
Extracts data from ClickHouse source database and loads into
PostgreSQL local warehouse.

- Full refresh for dimension/master tables
- Incremental load for the large event tracking table

Usage:
    python ingestion/clickhouse_to_postgres.py
"""

import os
import sys
import logging
from datetime import datetime

import clickhouse_connect
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Tables to full-refresh every run (small dimension / master tables)
FULL_REFRESH_TABLES = [
    "brand",
    "cpu_model",
    "gpu_model",
    "laptop_model",
    "laptop_benchmark_result",
]

# Tables to load incrementally (large fact tables)
INCREMENTAL_TABLES = {
    "user_event_tracking": "event_received_on_server_timestamp",
}

RAW_SCHEMA = "raw"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_clickhouse_client():
    """Create a ClickHouse client from environment variables."""
    return clickhouse_connect.get_client(
        host=require_env("CLICKHOUSE_HOST"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8443")),
        username=require_env("CLICKHOUSE_USER"),
        password=require_env("CLICKHOUSE_PASSWORD"),
        database=os.getenv("CLICKHOUSE_DATABASE", "laplaptech"),
        secure=False,  # port 80 = HTTP, not HTTPS
    )


def get_postgres_engine():
    """Create a SQLAlchemy engine for PostgreSQL."""
    user = require_env("POSTGRES_USER")
    password = require_env("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DATABASE", "laplaptech_pipeline")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


def ensure_database_exists():
    """Create the target PostgreSQL database if it does not exist."""
    user = require_env("POSTGRES_USER")
    password = require_env("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DATABASE", "laplaptech_pipeline")

    # Connect to default 'postgres' database to create target database
    admin_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")

    with admin_engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db"),
            {"db": database},
        )
        if not result.fetchone():
            conn.execute(text(f'CREATE DATABASE "{database}"'))
            logger.info(f"Created database: {database}")
        else:
            logger.info(f"Database already exists: {database}")

    admin_engine.dispose()


def ensure_schema_exists(engine):
    """Create the raw schema if it does not exist."""
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))
        conn.commit()
    logger.info(f"Schema '{RAW_SCHEMA}' ready")


# ---------------------------------------------------------------------------
# Extraction & Loading
# ---------------------------------------------------------------------------


def extract_full_table(ch_client, table_name: str) -> pd.DataFrame:
    """Extract an entire table from ClickHouse."""
    query = f"SELECT * FROM {table_name}"
    logger.info(f"Extracting full table: {table_name}")
    result = ch_client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    logger.info(f"  → {len(df):,} rows extracted")
    return df


def extract_incremental(
    ch_client, table_name: str, timestamp_col: str, engine
) -> pd.DataFrame:
    """Extract only new rows from ClickHouse since the last loaded timestamp."""
    # Get the max timestamp already loaded in PostgreSQL
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
        # Table doesn't exist yet — will do full load
        pass

    if max_ts:
        query = f"SELECT * FROM {table_name} WHERE {timestamp_col} > '{max_ts}'"
        logger.info(
            f"Extracting incremental: {table_name} (after {max_ts})"
        )
    else:
        query = f"SELECT * FROM {table_name}"
        logger.info(
            f"Extracting full (first load): {table_name}"
        )

    result = ch_client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    logger.info(f"  → {len(df):,} rows extracted")
    return df


def load_to_postgres(
    df: pd.DataFrame, table_name: str, engine, if_exists: str = "replace"
):
    """Load a DataFrame into PostgreSQL."""
    if df.empty:
        logger.info(f"  → No new data for {table_name}, skipping load")
        return

    logger.info(f"Loading {len(df):,} rows into {RAW_SCHEMA}.{table_name}...")
    
    # Custom replace logic to handle dependent dbt views
    if if_exists == "replace":
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{table_name} CASCADE"))
        if_exists = "append"

    df.to_sql(
        name=table_name,
        con=engine,
        schema=RAW_SCHEMA,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"  → Loaded successfully")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run_pipeline():
    """Execute the full ingestion pipeline."""
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("LaplapTech Pipeline — Ingestion Start")
    logger.info("=" * 60)

    # 1. Ensure PostgreSQL database & schema exist
    ensure_database_exists()
    engine = get_postgres_engine()
    ensure_schema_exists(engine)

    # 2. Connect to ClickHouse
    ch_client = get_clickhouse_client()
    logger.info(f"Connected to ClickHouse: {os.getenv('CLICKHOUSE_HOST')}")

    # 3. Full refresh tables
    for table_name in FULL_REFRESH_TABLES:
        try:
            df = extract_full_table(ch_client, table_name)
            load_to_postgres(df, table_name, engine, if_exists="replace")
        except Exception as e:
            logger.error(f"Error processing {table_name}: {e}")
            raise

    # 4. Incremental tables
    for table_name, ts_col in INCREMENTAL_TABLES.items():
        try:
            df = extract_incremental(ch_client, table_name, ts_col, engine)
            # First load = replace, subsequent = append
            mode = "replace" if df.shape[0] > 0 else "append"
            # Check if table exists to decide mode
            try:
                with engine.connect() as conn:
                    result = conn.execute(
                        text(
                            f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table_name}"
                        )
                    )
                    existing_count = result.fetchone()[0]
                    if existing_count > 0:
                        mode = "append"
                    else:
                        mode = "replace"
            except Exception:
                mode = "replace"

            load_to_postgres(df, table_name, engine, if_exists=mode)
        except Exception as e:
            logger.error(f"Error processing {table_name}: {e}")
            raise

    # 5. Summary
    elapsed = datetime.now() - start
    logger.info("=" * 60)
    logger.info(f"Ingestion completed in {elapsed}")

    # Print row counts
    with engine.connect() as conn:
        for table in FULL_REFRESH_TABLES + list(INCREMENTAL_TABLES.keys()):
            try:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table}")
                )
                count = result.fetchone()[0]
                logger.info(f"  {table}: {count:,} rows")
            except Exception:
                logger.info(f"  {table}: (not found)")

    logger.info("=" * 60)
    engine.dispose()


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
