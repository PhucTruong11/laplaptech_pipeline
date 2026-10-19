"""Unit tests offline và integration tests trên PostgreSQL local riêng.

TEST_DATABASE_URL chỉ được phép localhost + database laplaptech_pipeline_test.
Mỗi test sử dụng schema ngẫu nhiên; không truy cập ClickHouse hoặc Neon.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from decimal import Decimal
import os
from pathlib import Path
import threading
import unittest
from unittest.mock import Mock, patch
from uuid import uuid4

import pandas as pd
from jinja2 import Environment
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from ingestion import clickhouse_to_postgres as ingestion

TS = "event_received_on_server_timestamp"


def events(timestamp=200, payload="new", event_id=1):
    return pd.DataFrame([{"id": event_id, TS: timestamp, "payload": payload}])


class BatchTests(unittest.TestCase):
    def test_latest_version_wins_in_either_order_without_mutating_input(self):
        batch = pd.concat([events(), events(100, "old")], ignore_index=True)
        before = batch.copy(deep=True)
        for data in (batch, batch.iloc[::-1]):
            result = ingestion.prepare_event_batch(data)
            self.assertEqual(result.iloc[0]["payload"], "new")
        pd.testing.assert_frame_equal(batch, before)

    def test_identical_replays_are_deduplicated(self):
        result = ingestion.prepare_event_batch(pd.concat([events(), events()]))
        self.assertEqual(len(result), 1)

    def test_conflicting_latest_version_fails(self):
        with self.assertRaisesRegex(ValueError, "payload"):
            ingestion.prepare_event_batch(pd.concat([events(), events(payload="conflict")]))

    def test_invalid_ids_and_watermarks_fail(self):
        for key, value in [("id", None), ("id", ""), ("id", "  "), (TS, None), (TS, float("inf")), (TS, "bad")]:
            with self.subTest(key=key, value=value), self.assertRaises((ValueError, TypeError)):
                batch = events()
                batch[key] = value
                ingestion.prepare_event_batch(batch)

    def test_json_objects_are_serialized_before_dedup(self):
        batch = events().drop(columns="payload")
        batch["event_data"] = [{"b": 2, "a": 1}]
        result = ingestion.prepare_event_batch(pd.concat([batch, batch]))
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["event_data"], '{"a": 1, "b": 2}')

    def test_empty_batch_does_not_connect(self):
        engine = Mock()
        ingestion.load_incremental_to_postgres(events().iloc[:0], "user_event_tracking", engine)
        engine.begin.assert_not_called()


class ConfigurationTests(unittest.TestCase):
    def test_password_round_trips_without_url_escaping(self):
        password = "p@ss:/?#%word"
        with patch.dict(os.environ, {"POSTGRES_USER": "test", "POSTGRES_PASSWORD": password}):
            url = ingestion.postgres_url()
            self.assertEqual(make_url(url.render_as_string(hide_password=False)).password, password)

    def test_managed_database_is_not_provisioned_by_default(self):
        with patch.dict(os.environ, {"POSTGRES_CREATE_DATABASE": "false"}), patch.object(ingestion, "create_engine") as create:
            ingestion.ensure_database_exists()
            create.assert_not_called()

    def test_clickhouse_tls_follows_port_unless_explicitly_configured(self):
        config = {"CLICKHOUSE_HOST": "test", "CLICKHOUSE_USER": "test", "CLICKHOUSE_PASSWORD": "test", "CLICKHOUSE_SECURE": ""}
        with patch.dict(os.environ, config), patch.object(ingestion.clickhouse_connect, "get_client") as create:
            for port, secure in [("80", False), ("8443", True)]:
                with patch.dict(os.environ, {"CLICKHOUSE_PORT": port}):
                    ingestion.get_clickhouse_client()
                    self.assertEqual(create.call_args.kwargs["secure"], secure)

    def test_backfill_requires_timezone_and_a_valid_complete_range(self):
        self.assertEqual(ingestion.parse_epoch("1970-01-01T07:00:00+07:00"), 0)
        with self.assertRaises(argparse.ArgumentTypeError):
            ingestion.parse_epoch("2026-10-01")
        for start, end in [(1, None), (None, 2), (2, 1), (1, 1), (float("nan"), 2)]:
            with self.subTest(start=start, end=end), self.assertRaises(ValueError):
                ingestion.validate_range(start, end)

    def test_backfill_does_not_read_current_watermark(self):
        engine, client = Mock(), Mock()
        client.query.return_value = Mock(result_rows=[], column_names=["id", TS])
        ingestion.extract_incremental(client, "user_event_tracking", TS, engine, start_epoch=1, end_epoch=2)
        engine.connect.assert_not_called()
        self.assertEqual(client.query.call_args.kwargs["parameters"], {"start": 1, "end": 2})
        self.assertIn(f"{TS} < {{end:Float64}}", client.query.call_args.args[0])

    def test_resources_are_closed_if_extraction_fails(self):
        engine, client = Mock(), Mock()
        with (
            patch.object(ingestion, "ensure_database_exists"),
            patch.object(ingestion, "get_postgres_engine", return_value=engine),
            patch.object(ingestion, "ensure_schema_exists"),
            patch.object(ingestion, "get_clickhouse_client", return_value=client),
            patch.object(ingestion, "extract_full_table", side_effect=RuntimeError("source unavailable")),
        ):
            with self.assertRaisesRegex(RuntimeError, "source unavailable"):
                ingestion.run_pipeline()
        client.close.assert_called_once()
        engine.dispose.assert_called_once()


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL để chạy PostgreSQL integration tests")
class PostgresTests(unittest.TestCase):
    def setUp(self):
        url = make_url(os.environ["TEST_DATABASE_URL"])
        if url.host not in {"localhost", "127.0.0.1"} or url.database != "laplaptech_pipeline_test":
            raise RuntimeError("Chỉ cho phép PostgreSQL local database laplaptech_pipeline_test")
        self.engine = create_engine(url, pool_pre_ping=True)
        self.addCleanup(self.engine.dispose)
        self.schema = "test_ingestion_" + uuid4().hex
        schema_patch = patch.object(ingestion, "RAW_SCHEMA", self.schema)
        schema_patch.start()
        self.addCleanup(schema_patch.stop)
        ingestion.ensure_schema_exists(self.engine)
        self.addCleanup(self.drop_test_schema)

    def drop_test_schema(self):
        with self.engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA "{self.schema}" CASCADE'))

    def relation(self, table):
        return f'"{self.schema}"."{table}"'

    def rows(self, table):
        with self.engine.connect() as conn:
            return conn.execute(text(f"SELECT * FROM {self.relation(table)} ORDER BY id")).all()

    def assert_no_staging(self):
        with self.engine.connect() as conn:
            names = conn.execute(text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = :schema
            """), {"schema": self.schema}).scalars().all()
        self.assertFalse(any(name.startswith("_stg_") for name in names))

    def seed_dimension(self):
        ingestion.load_to_postgres(pd.DataFrame([{"id": 1, "name": "old"}]), "brand", self.engine, truncate_first=True)

    def test_full_refresh_rolls_back_insert_failure_and_preserves_view(self):
        self.seed_dimension()
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE VIEW {self.relation("brand_view")} AS SELECT * FROM {self.relation("brand")}'))
        bad = pd.DataFrame([{"id": "not_an_integer", "name": "new"}])
        with self.assertRaises((SQLAlchemyError, pd.errors.DatabaseError)):
            ingestion.load_to_postgres(bad, "brand", self.engine, truncate_first=True)
        self.assertEqual(self.rows("brand"), [(1, "old")])
        self.assertEqual(self.rows("brand_view"), [(1, "old")])
        ingestion.load_to_postgres(pd.DataFrame([{"id": 2, "name": "new"}]), "brand", self.engine, truncate_first=True)
        self.assertEqual(self.rows("brand_view"), [(2, "new")])

    def test_empty_dimension_and_schema_change_preserve_old_data(self):
        self.seed_dimension()
        for batch in [pd.DataFrame(columns=["id", "name"]), pd.DataFrame([{"id": 2, "renamed": "new"}])]:
            with self.assertRaises(ValueError):
                ingestion.load_to_postgres(batch, "brand", self.engine, truncate_first=True)
            self.assertEqual(self.rows("brand"), [(1, "old")])

    def test_replay_older_batch_and_new_version(self):
        load = lambda frame: ingestion.load_incremental_to_postgres(frame, "user_event_tracking", self.engine)
        load(events())
        original = self.rows("user_event_tracking")
        load(events())
        load(events(100, "old"))
        self.assertEqual(self.rows("user_event_tracking"), original)
        # Source column order may change; explicit INSERT columns must preserve data.
        load(events(300, "newest")[["payload", TS, "id"]])
        self.assertEqual(self.rows("user_event_tracking"), [(1, 300, "newest")])
        self.assert_no_staging()

    def test_equal_timestamp_conflict_rolls_back_and_drops_staging(self):
        ingestion.load_incremental_to_postgres(events(), "user_event_tracking", self.engine)
        with self.assertRaisesRegex(ValueError, "payload"):
            ingestion.load_incremental_to_postgres(events(payload="conflict"), "user_event_tracking", self.engine)
        self.assertEqual(self.rows("user_event_tracking"), [(1, 200, "new")])
        self.assert_no_staging()

    def test_failure_after_delete_rolls_back_raw_and_staging(self):
        ingestion.load_incremental_to_postgres(events(), "user_event_tracking", self.engine)
        with self.engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {self.relation('user_event_tracking')} ADD CHECK (payload <> 'bad')"))
        with self.assertRaises(SQLAlchemyError):
            ingestion.load_incremental_to_postgres(events(300, "bad"), "user_event_tracking", self.engine)
        self.assertEqual(self.rows("user_event_tracking"), [(1, 200, "new")])
        self.assert_no_staging()

    def test_concurrent_writers_keep_newest_version(self):
        ingestion.load_incremental_to_postgres(events(100, "old"), "user_event_tracking", self.engine)
        barrier = threading.Barrier(2)

        def load(timestamp):
            barrier.wait(timeout=10)
            ingestion.load_incremental_to_postgres(events(timestamp, str(timestamp)), "user_event_tracking", self.engine)

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(load, ts) for ts in (200, 300)]
            for future in futures:
                future.result(timeout=40)
        self.assertEqual(self.rows("user_event_tracking"), [(1, 300, "300")])
        self.assert_no_staging()

    def test_missing_table_full_extract_and_decimal_watermark_lookback(self):
        client = Mock()
        client.query.return_value = Mock(result_rows=[], column_names=["id", TS])
        ingestion.extract_incremental(client, "user_event_tracking", TS, self.engine)
        self.assertNotIn("WHERE", client.query.call_args.args[0])
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE TABLE {self.relation("user_event_tracking")} (id bigint, "{TS}" numeric)'))
            conn.execute(text(f'INSERT INTO {self.relation("user_event_tracking")} VALUES (1, 10000.5)'))
        with patch.dict(os.environ, {"INGESTION_LOOKBACK_SECONDS": "7200"}):
            ingestion.extract_incremental(client, "user_event_tracking", TS, self.engine)
        self.assertEqual(client.query.call_args.kwargs["parameters"]["watermark"], float(Decimal("2800.5")))

    def test_watermark_query_errors_do_not_trigger_full_extract(self):
        client = Mock()
        with self.engine.begin() as conn:
            conn.execute(text(f'CREATE TABLE {self.relation("user_event_tracking")} (id bigint)'))
        with self.assertRaises(SQLAlchemyError):
            ingestion.extract_incremental(client, "user_event_tracking", TS, self.engine)
        client.query.assert_not_called()

    def test_daily_kpi_counts_events_and_sessions_across_vietnam_midnight(self):
        with self.engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE {self.relation('tracking')} (
                    server_timestamp timestamptz, session_id text, event_name text,
                    page_name text, sort_by text
                )
            """))
            conn.execute(text(f"""
                INSERT INTO {self.relation('tracking')} VALUES
                ('2026-10-18T16:59:00Z', 's1', 'pageview', 'Home', NULL),
                ('2026-10-18T17:01:00Z', 's1', 'pageview', 'DeviceDetail', NULL),
                ('2026-10-18T17:02:00Z', 's1', 'pageview', 'CompareDevice', NULL),
                ('2026-10-18T17:03:00Z', 's2', 'pageview', 'CompareDevice', NULL)
            """))
            path = Path(__file__).resolve().parents[1] / "dbt/models/gold/gold_daily_site_kpis.sql"
            root = path.parents[3]
            env = Environment()
            macros = env.from_string((root / 'dbt/macros/business_time.sql').read_text()).make_module(
                {'var': lambda name, default: default})
            day_sql = env.from_string((root / 'dbt/models/silver/silver_session_day.sql').read_text()).render(
                ref=lambda name: self.relation('tracking'), business_date=macros.business_date)
            conn.execute(text(f'CREATE VIEW {self.relation("session_day")} AS {day_sql}'))
            sql = env.from_string(path.read_text(encoding='utf-8')).render(
                ref=lambda name: self.relation('session_day'))
            rows = {r["event_date"]: r for r in conn.execute(text(sql)).mappings()}
        self.assertEqual(rows[date(2026, 10, 18)]["total_events"], 1)
        today = rows[date(2026, 10, 19)]
        self.assertEqual(today["total_sessions"], 2)
        self.assertEqual(today["total_events"], 3)
        self.assertEqual(today["detail_rate_pct"], Decimal("50.00"))
        self.assertEqual(today["comparison_rate_pct"], Decimal("100.00"))
        self.assertIsNone(rows[date(2026, 10, 18)]["comparison_rate_pct"])


if __name__ == "__main__":
    unittest.main()
