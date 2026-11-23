"""Run a bounded LapLapTech recovery during a local maintenance window."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text

from ingestion.clickhouse_to_postgres import parse_epoch, validate_range
from ingestion.settings import read_lookback_seconds
from scripts.run_airflow_stage import lab_environment, run_stage, verify_database, warehouse_url


def recovery_epochs(start: str, end: str) -> tuple[float, float]:
    epochs = parse_epoch(start), parse_epoch(end)
    validate_range(*epochs)
    return epochs


def execute_recovery(start: str, end: str, execute) -> int:
    """Run the same stage sequence in the Docker runtime and the fixture."""
    recovery_epochs(start, end)
    for stage, options in (
        ('ingest', {'backfill': (start, end)}),
        ('freshness', {}),
        ('build', {'full_refresh': True}),
        ('reconcile', {}),
    ):
        status = execute(stage, **options)
        if status:
            return status
    return 0


def raw_snapshot(connection, epochs: tuple[float, float]) -> dict | None:
    if connection.execute(text("SELECT to_regclass('raw.user_event_tracking')")).scalar_one() is None:
        return None
    row = connection.execute(text("""
        SELECT count(*) AS events, max(event_received_on_server_timestamp) AS watermark,
               count(*) FILTER (WHERE event_received_on_server_timestamp >= :start
                                AND event_received_on_server_timestamp < :end) AS events_in_range
        FROM raw.user_event_tracking
    """), {'start': epochs[0], 'end': epochs[1]}).mappings().one()
    return dict(row)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', required=True, help='Inclusive ISO-8601 timestamp, with timezone')
    parser.add_argument('--end', required=True, help='Exclusive ISO-8601 timestamp, with timezone')
    args = parser.parse_args()
    try:
        epochs = recovery_epochs(args.start, args.end)
    except (argparse.ArgumentTypeError, ValueError) as error:
        parser.error(str(error))

    run_id = 'recovery-' + uuid4().hex
    run_key = hashlib.sha256(run_id.encode()).hexdigest()[:20]
    output = Path('/artifacts') / run_key
    output.mkdir(parents=True, exist_ok=False)
    report = {
        'run_id': run_id, 'status': 'failed', 'started_at': datetime.now(timezone.utc).isoformat(),
        'start_inclusive': args.start, 'end_exclusive': args.end, 'stages': {},
    }
    engine = None
    try:
        env = lab_environment()
        verify_database(env)
        report['lookback_seconds'] = read_lookback_seconds(env)
        engine = create_engine(warehouse_url(env), connect_args={'connect_timeout': 10},
                               isolation_level='AUTOCOMMIT')
        with engine.connect() as connection:
            # Session lock prevents concurrent recoveries, including from another
            # client. Closing this dedicated connection releases it.
            if not connection.execute(text('SELECT pg_try_advisory_lock(74622, 1)')).scalar_one():
                print('Another LapLapTech recovery holds the warehouse lock.', flush=True)
                return 1
            try:
                report['raw_before'] = raw_snapshot(connection, epochs)

                def execute(stage, **options):
                    report['stages'][stage] = {'status': 'failed'}
                    code = run_stage(stage, run_id=run_id, env=env, **options)
                    report['stages'][stage].update(status='success' if code == 0 else 'failed',
                                                   exit_code=code)
                    return code

                code = execute_recovery(args.start, args.end, execute)
                if code == 0:
                    report['raw_after'] = raw_snapshot(connection, epochs)
            finally:
                connection.execute(text('SELECT pg_advisory_unlock(74622, 1)'))
        report['status'] = 'success' if code == 0 else 'failed'
        return code
    except Exception as error:
        report['error_type'] = type(error).__name__
        print(f'Recovery stopped: {type(error).__name__}. Inspect the stage reports.', flush=True)
        return 1
    finally:
        if engine is not None:
            engine.dispose()
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        (output / 'recovery.json').write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
        print(f'Recovery artifacts: {output}', flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
