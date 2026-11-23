"""Execute and verify dbt model tasks for the published LapLapTech release."""

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
from uuid import uuid4

from scripts.run_airflow_stage import lab_environment, verify_database


MANIFEST = Path('/app/cosmos/manifest.json')
ARTIFACTS = Path('/artifacts')


def run_directory(root: Path, run_id: str) -> Path:
    return root / sha256(run_id.encode()).hexdigest()[:20]


def release_nodes(path: Path, release: str) -> tuple[dict, set[str]]:
    contents = path.read_bytes()
    if sha256(contents).hexdigest() != release:
        raise ValueError('Runtime dbt manifest differs from the published release')
    nodes = json.loads(contents)['nodes']
    models = {key: node for key, node in nodes.items() if node['resource_type'] == 'model'}
    tests = {key for key, node in nodes.items() if node['resource_type'] == 'test'}
    if not models or not tests:
        raise ValueError('A release needs enabled dbt models and data tests')
    return models, tests


def cgroup_limits() -> dict:
    base = Path('/sys/fs/cgroup')
    return {key: (base / key).read_text().strip() for key in ('memory.max', 'cpu.max', 'pids.max')
            if (base / key).is_file()}


def write_report(path: Path, report: dict) -> None:
    pending = path.with_name(f'.{path.name}.{uuid4().hex}')
    try:
        pending.write_text(json.dumps(report, indent=2), encoding='utf-8')
        pending.replace(path)
    finally:
        pending.unlink(missing_ok=True)


def latest_attempt(base: Path, key: str, release: str, run_id: str) -> dict:
    reports = [json.loads(path.read_text(encoding='utf-8'))
               for path in (base / 'cosmos' / key).glob('*/report.json')]
    if not reports:
        raise ValueError(f'Missing Cosmos report for {key}')
    report = max(reports, key=lambda row: (row['started_at'], row['attempt']))
    if (report['status'] != 'success' or report['release'] != release
            or report['run_id'] != run_id or not report['task_id']):
        raise ValueError(f'Latest Cosmos attempt is not valid for {key}')
    return report


def collect(base: Path, release: str, run_id: str, models: dict, tests: set[str], task_id: str) -> dict:
    model_runs = {}
    for unique_id in models:
        report = latest_attempt(base, unique_id, release, run_id)
        if report['results'] != {unique_id: 'success'}:
            raise ValueError(f'Model result mismatch: {unique_id}')
        model_runs[unique_id] = report
    test_report = latest_attempt(base, 'tests', release, run_id)
    if test_report['results'] != dict.fromkeys(tests, 'pass'):
        raise ValueError('dbt test outcomes differ from the release inventory')
    task_ids = [report['task_id'] for report in model_runs.values()] + [test_report['task_id'], task_id]
    if len(set(task_ids)) != len(task_ids):
        raise ValueError('Each release result must have its own Airflow task')
    refresh_modes = {report['full_refresh'] for report in model_runs.values()}
    if len(refresh_modes) != 1:
        raise ValueError('Model attempts disagree on full refresh')
    return {
        'status': 'success', 'release': release, 'run_id': run_id,
        'full_refresh': refresh_modes.pop(), 'model_count': len(models), 'test_count': len(tests),
        'models': model_runs, 'tests': test_report,
        'collector': {'task_id': task_id, 'resource_limits': cgroup_limits()},
    }


def execute_dbt(base: Path, *, release: str, run_id: str, task_id: str, attempt: int, env: dict,
                key: str, dbt_args: list[str], expected: dict, full_refresh: bool) -> int:
    output = base / 'cosmos' / key / f'attempt-{attempt}-{uuid4().hex[:8]}'
    output.mkdir(parents=True, exist_ok=False)
    report = {
        'started_at': datetime.now(timezone.utc).isoformat(), 'attempt': attempt, 'run_id': run_id,
        'task_id': task_id, 'release': release, 'full_refresh': full_refresh,
        'status': 'failed', 'resource_limits': cgroup_limits(),
    }
    try:
        code = subprocess.run(
            ['dbt', *dbt_args, '--project-dir', '/app/dbt', '--profiles-dir', '/app/dbt',
             '--target-path', str(output / 'target'), '--log-path', str(output / 'logs')],
            cwd='/app', env=env, check=False,
        ).returncode
        if code:
            return code
        rows = json.loads((output / 'target/run_results.json').read_text(encoding='utf-8'))['results']
        results = {row['unique_id']: row['status'] for row in rows}
        report['results'] = results
        if len(rows) != len(expected) or results != expected:
            print(f'dbt result mismatch for {key}', flush=True)
            return 1
        report['status'] = 'success'
        return 0
    finally:
        write_report(output / 'report.json', report)
        print(f'Cosmos task artifacts: {output}', flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['model', 'test', 'collect'])
    parser.add_argument('--model')
    parser.add_argument('--full-refresh', action='store_true')
    args = parser.parse_args()
    if (args.action == 'model') != bool(args.model) or (args.full_refresh and args.action != 'model'):
        parser.error('Only model tasks accept --model and --full-refresh')
    release = os.environ['LAPLAPTECH_MANIFEST_SHA256']
    run_id, task_id = os.environ['PIPELINE_RUN_ID'], os.environ['PIPELINE_TASK_ID']
    attempt = int(os.environ['PIPELINE_ATTEMPT'])
    if attempt < 1 or not run_id or not task_id:
        raise ValueError('Cosmos tasks require a run ID, task ID and positive attempt')
    base = run_directory(ARTIFACTS, run_id)
    models, tests = release_nodes(MANIFEST, release)
    if args.action == 'collect':
        target = base / 'cosmos-summary.json'
        # An unsuccessful retry must not leave the preceding collector result current.
        target.unlink(missing_ok=True)
        summary = collect(base, release, run_id, models, tests, task_id)
        write_report(target, summary)
        print(f'Verified {len(models)} model tasks and {len(tests)} data tests: {target}', flush=True)
        return 0

    env = lab_environment()
    verify_database(env)
    if args.action == 'model':
        if args.model not in models:
            parser.error('The requested model is not in the release')
        dbt_args = ['run', '--select', 'fqn:' + '.'.join(models[args.model]['fqn'])]
        if args.full_refresh:
            dbt_args.append('--full-refresh')
        expected = {args.model: 'success'}
    else:
        dbt_args = ['test']
        expected = dict.fromkeys(tests, 'pass')
    return execute_dbt(base, release=release, run_id=run_id, task_id=task_id, attempt=attempt,
                       env=env, key=args.model or 'tests', dbt_args=dbt_args, expected=expected,
                       full_refresh=args.full_refresh)


if __name__ == '__main__':
    raise SystemExit(main())
