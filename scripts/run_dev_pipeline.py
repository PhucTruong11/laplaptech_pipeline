"""Run real ingestion + freshness + dbt builds against a LOCAL dev database only.

python -m scripts.run_dev_pipeline --source fixture|clickhouse
Set DEV_DATABASE_URL; fixture is the default and never reads remote credentials.
"""
import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import importlib.metadata
import json
import os
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from ingestion import clickhouse_to_postgres as ingestion
from scripts.dev_fixtures import FixtureClickHouse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=['fixture', 'clickhouse'], default='fixture')
    parser.add_argument('--allow-stale-source', action='store_true', help='Dev historical data only; record freshness failure and continue build')
    parser.add_argument('--reset-fixture', action='store_true', help='Reset only the named local fixture schemas before loading a changed fixture contract')
    args = parser.parse_args()
    url = make_url(os.environ['DEV_DATABASE_URL'])
    if url.host not in {'localhost', '127.0.0.1'} or url.database not in {'laplaptech_pipeline_dev', 'laplaptech_pipeline_test', 'laplaptech_pipeline_dev_live'}:
        raise RuntimeError('DEV_DATABASE_URL must point to an explicitly named LOCAL dev/test database')
    if args.reset_fixture:
        if args.source != 'fixture' or url.database == 'laplaptech_pipeline_dev_live':
            raise RuntimeError('Reset is restricted to the LOCAL fixture database, never dev-live')
        engine = create_engine(url)
        try:
            with engine.begin() as conn:
                for schema in ('public_gold', 'public_silver', 'public_bronze', 'raw'):
                    conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            engine.dispose()
    root = Path(__file__).resolve().parents[1]
    out = root / 'scratch' / ('dev-e2e-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    out.mkdir(parents=True)
    env = {
        'POSTGRES_HOST': url.host, 'POSTGRES_PORT': str(url.port or 5432),
        'POSTGRES_USER': url.username, 'POSTGRES_PASSWORD': url.password or '',
        'POSTGRES_DATABASE': url.database, 'POSTGRES_SSLMODE': 'prefer',
        'POSTGRES_CREATE_DATABASE': 'false', 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false',
    }
    report = {'source': args.source, 'database': url.database, 'started_at': datetime.now(timezone.utc).isoformat(),
              'stages': [], 'versions': {n: importlib.metadata.version(n) for n in ['dbt-core', 'dbt-postgres', 'pandas', 'streamlit']}}

    def snapshot():
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                names = conn.execute(text("""SELECT table_schema, table_name FROM information_schema.tables
                    WHERE table_schema IN ('raw','public_bronze','public_silver','public_gold') ORDER BY 1,2""")).all()
                return {f'{s}.{t}': conn.execute(text(f'SELECT COUNT(*) FROM "{s}"."{t}"')).scalar() for s, t in names}
        finally: engine.dispose()

    def check_fixture_boundaries():
        # Python reference from raw, independent of dbt/Jinja date expressions.
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                expected, sessions = Counter(), defaultdict(set)
                for row in conn.execute(text('SELECT session_id,event_name,event_received_on_server_timestamp FROM raw.user_event_tracking')):
                    if row.session_id is not None and row.event_name is not None:
                        day = datetime.fromtimestamp(float(row.event_received_on_server_timestamp), ZoneInfo('Asia/Ho_Chi_Minh')).date()
                        expected[day] += 1
                        sessions[day].add(row.session_id)
                actual = conn.execute(text('SELECT event_date,total_events,total_sessions FROM public_gold.gold_daily_site_kpis')).all()
                assert {r.event_date: r.total_events for r in actual} == dict(expected), 'Daily timezone/event counts mismatch'
                assert {r.event_date: r.total_sessions for r in actual} == {d: len(s) for d,s in sessions.items()}, 'Session-day counts mismatch'
                months = conn.execute(text('SELECT DISTINCT month FROM public_gold.gold_cpu_trend')).scalars().all()
                assert len(months) >= 2, 'Month-boundary fixture lost a low-volume month'
                assert conn.execute(text('SELECT COUNT(*) FROM public_silver.silver_comparison_event_device')).scalar() > 0
                assert conn.execute(text('SELECT MAX(step_3_select_compare) FROM public_gold.gold_behavior_funnel_daily')).scalar() > 0
                assert conn.execute(text('SELECT MAX(step_5_sort_action) FROM public_gold.gold_behavior_funnel_daily')).scalar() > 0
                assert conn.execute(text('SELECT geekbench6_multi FROM public_gold.gold_performance_ranking WHERE laptop_model_id=1')).scalar() == 10000
                report['fixture_boundary_checks_pass'] = True
        finally:
            engine.dispose()

    try:
        with patch.dict(os.environ, env):
            # Fixture injection changes only the source client; extraction and loaders are real.
            fixture = patch.object(ingestion, 'get_clickhouse_client', side_effect=FixtureClickHouse)
            if args.source == 'fixture': fixture.start()
            try:
                ingestion.run_pipeline()
                from dbt.cli.main import dbtRunner

                def dbt(arguments):
                    result = dbtRunner().invoke(arguments + [
                        '--project-dir', str(root/'dbt'), '--profiles-dir', str(root/'dbt'),
                        '--target-path', str(out/'target'), '--log-path', str(out/'logs')])
                    report['stages'].append({'command': ' '.join(arguments), 'success': result.success})
                    if result.exception: report['stages'][-1]['exception_type'] = type(result.exception).__name__
                    return result.success

                fresh = dbt(['source', 'freshness'])
                if not fresh and not args.allow_stale_source:
                    raise RuntimeError('Source freshness failed; inspect sources.json before build')
                if not dbt(['build', '--full-refresh']): raise RuntimeError('Full dbt build failed')
                first = snapshot()
                ingestion.run_pipeline()  # real replay through watermark + staging
                if not dbt(['build']): raise RuntimeError('Incremental replay build failed')
                second = snapshot()
                if first != second: raise RuntimeError('Replay changed relation row counts')
                if args.source == 'fixture': check_fixture_boundaries()
                report['row_counts'] = second
                report['replay_row_counts_unchanged'] = True
                manifest = json.loads((out/'target/manifest.json').read_text(encoding='utf-8'))
                result = json.loads((out/'target/run_results.json').read_text(encoding='utf-8'))
                report['model_count'] = sum(n['resource_type'] == 'model' for n in manifest['nodes'].values())
                report['data_test_count'] = sum(n['resource_type'] == 'test' for n in manifest['nodes'].values())
                report['test_pass_count'] = sum(r['status'] == 'pass' for r in result['results'])
                report['success'] = True
            finally:
                if args.source == 'fixture': fixture.stop()
    finally:
        report.setdefault('success', False)
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        (out/'report.json').write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
        print('DEV_REPORT:', out/'report.json')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
