"""Run one pipeline stage against the explicitly configured local warehouse."""

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
        'POSTGRES_USER': 'laplaptech_pipeline',
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
    threads = config.get('DBT_THREADS', env.get('DBT_THREADS', '2'))
    if not str(threads).isdigit() or not 1 <= int(threads) <= 16:
        raise RuntimeError('DBT_THREADS must be an integer from 1 to 16')
    env['DBT_THREADS'] = str(threads)
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
            database, username, admin = connection.execute(
                text('SELECT current_database(), current_user, rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls FROM pg_roles WHERE rolname=current_user')
            ).one()
        if (database, username) != ('laplaptech_pipeline_dev', 'laplaptech_pipeline') or admin:
            raise RuntimeError('Lab database identity did not match')
    finally:
        engine.dispose()


def stage_command(stage: str, output: Path, *, full_refresh: bool = False) -> list[str]:
    if stage == 'ingest':
        return [sys.executable, '-m', 'ingestion.clickhouse_to_postgres']
    arguments = ['source', 'freshness'] if stage == 'freshness' else ['build']
    if full_refresh:
        arguments.append('--full-refresh')
    return ['dbt', *arguments,
            '--project-dir', '/app/dbt', '--profiles-dir', '/app/dbt',
            '--target-path', str(output / 'target'),
            '--log-path', str(output / 'logs')]


def reconcile_pipeline(env: dict[str, str]) -> dict:
    """Read-only reconciliation independent of the transformation expressions."""
    url = URL.create(
        'postgresql+psycopg2', username=env['POSTGRES_USER'], password=env['POSTGRES_PASSWORD'],
        host=env['POSTGRES_HOST'], port=int(env['POSTGRES_PORT']),
        database=env['POSTGRES_DATABASE'], query={'sslmode': env['POSTGRES_SSLMODE']},
    )
    engine = create_engine(url, connect_args={'connect_timeout': 10})
    try:
        with engine.connect() as connection:
            relations = connection.execute(text("""
                SELECT table_schema, table_name FROM information_schema.tables
                WHERE table_schema IN ('raw','public_bronze','public_silver','public_gold')
                ORDER BY table_schema, table_name
            """)).all()
            expected = {'raw': 6, 'public_bronze': 6, 'public_silver': 13, 'public_gold': 12}
            schema_counts = {schema: sum(s == schema for s, _ in relations) for schema in expected}
            if schema_counts != expected:
                raise RuntimeError(f'Unexpected model inventory: {schema_counts}')
            quote = connection.dialect.identifier_preparer.quote_identifier
            row_counts = {
                f'{schema}.{name}': connection.execute(text(
                    f'SELECT COUNT(*) FROM {quote(schema)}.{quote(name)}'
                )).scalar_one() for schema, name in relations
            }
            duplicates = connection.execute(text("""
                SELECT count(*) - count(DISTINCT id) FROM raw.user_event_tracking
            """)).scalar_one()
            expected_silver = connection.execute(text("""
                SELECT count(*) FROM raw.user_event_tracking
                WHERE event_name IS NOT NULL AND session_id IS NOT NULL
            """)).scalar_one()
            daily = connection.execute(text("""
                SELECT count(*) FROM (
                    SELECT (server_timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS day,
                           count(*) AS events, count(DISTINCT session_id) AS sessions
                    FROM public_silver.silver_user_event_tracking GROUP BY 1
                ) s FULL JOIN public_gold.gold_daily_site_kpis g ON g.event_date=s.day
                WHERE s.day IS NULL OR g.event_date IS NULL
                   OR s.events IS DISTINCT FROM g.total_events
                   OR s.sessions IS DISTINCT FROM g.total_sessions
            """)).scalar_one()
            gold_events = connection.execute(text("""
                SELECT COALESCE(sum(total_events),0) FROM public_gold.gold_daily_site_kpis
            """)).scalar_one()
            raw_events = row_counts['raw.user_event_tracking']
            silver_events = row_counts['public_silver.silver_user_event_tracking']
            if duplicates or expected_silver != silver_events or daily:
                raise RuntimeError('Raw/Silver ID counts or daily timezone/session totals do not reconcile')
            if raw_events != row_counts['public_bronze.bronze_user_event_tracking'] or gold_events != silver_events:
                raise RuntimeError('Raw/Bronze or Silver/Gold event totals do not reconcile')
            return {'schema_counts': schema_counts, 'row_counts': row_counts,
                    'raw_events': raw_events, 'silver_events': silver_events,
                    'gold_daily_events': int(gold_events), 'duplicate_raw_ids': int(duplicates),
                    'daily_metric_mismatches': int(daily)}
    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['preflight', 'ingest', 'freshness', 'build', 'reconcile'])
    parser.add_argument('--full-refresh', action='store_true', help='Rebuild dbt incremental models; build stage only')
    args = parser.parse_args()
    if args.full_refresh and args.stage != 'build':
        parser.error('--full-refresh is only supported by the build stage')
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
    cgroup = Path('/sys/fs/cgroup')
    report['resource_limits'] = {name: (cgroup/name).read_text().strip()
                                 for name in ('memory.max', 'cpu.max', 'pids.max') if (cgroup/name).is_file()}
    try:
        env = lab_environment()
        verify_database(env)  # Read-only target check before every stage.
        print('Target verified: warehouse-laplaptech / laplaptech_pipeline_dev', flush=True)
        if args.stage == 'ingest':
            password = env.get('CLICKHOUSE_PASSWORD', '')
            if not password or password == 'REPLACE_WITH_PUBLIC_DATASET_PASSWORD':
                raise RuntimeError('Fill the public ClickHouse login before ingestion')
        if args.stage == 'preflight':
            code = 0
        elif args.stage == 'reconcile':
            report['reconciliation'] = reconcile_pipeline(env)
            print(json.dumps(report['reconciliation'], default=str), flush=True)
            code = 0
        else:
            code = subprocess.run(
                stage_command(args.stage, output, full_refresh=args.full_refresh),
                cwd='/app', env=env, check=False,
            ).returncode
        report.update(status='success' if code == 0 else 'failed', exit_code=code)
        return code
    finally:
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        (output / 'report.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
        print(f'Stage artifacts: {output}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
