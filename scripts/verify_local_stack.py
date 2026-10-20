"""Verify the local Docker stack; optionally run the real Airflow pipeline.

Requires Python 3.10+ and the Docker CLI, with no host Python packages.
Default: inspect services, imports, data, images and dashboard without running ETL.
--run-pipeline: trigger one incremental DAG run and validate its persisted artifacts.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import urlopen


PROJECT = Path(__file__).resolve().parents[1]
SERVICES = ('metadata', 'warehouse-laplaptech', 'airflow-api-server',
            'airflow-scheduler', 'airflow-dag-processor', 'dashboard')
DAG_ID = 'laplaptech_pipeline'
RUNTIME_IMAGE = 'laplaptech-runtime:local-v1'


def command(arguments: list[str], *, stdin: str | None = None, timeout: int = 90) -> str:
    result = subprocess.run(arguments, input=stdin, capture_output=True,
                            text=True, encoding='utf-8', errors='replace', timeout=timeout)
    if result.returncode:
        # A partial inspect result can contain container environment secrets.
        output = '' if arguments[:2] == ['docker', 'inspect'] else result.stdout[-1500:]
        raise RuntimeError(f'{arguments[:4]} exited {result.returncode}: '
                           f'{output} {result.stderr[-1500:]}')
    return result.stdout.strip()


def container(service: str) -> str:
    return f'airflow-platform-{service}-1'


def metadata(sql: str) -> str:
    return command(['docker', 'exec', container('metadata'), 'psql', '-v',
                    'ON_ERROR_STOP=1', '-U', 'airflow', '-d', 'airflow_meta', '-Atc', sql])


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def health_snapshot() -> dict:
    details = json.loads(command(['docker', 'inspect', *map(container, SERVICES)]))
    return {item['Name'].lstrip('/'): {
        'state': item['State']['Status'],
        'health': item['State'].get('Health', {}).get('Status'),
        'image_id': item['Image'],
    } for item in details}


def wait_healthy(timeout: int = 240) -> dict:
    deadline = time.monotonic() + timeout
    previous = None
    while True:
        snapshot = health_snapshot()
        states = {key: (value['state'], value['health']) for key, value in snapshot.items()}
        if states != previous:
            print('Services:', json.dumps(states), flush=True)
            previous = states
        if all(value == ('running', 'healthy') for value in states.values()):
            return snapshot
        if time.monotonic() >= deadline:
            raise RuntimeError('Services did not become healthy before the deadline')
        time.sleep(3)


def wait_run(run_id: str, timeout: int) -> dict:
    deadline = time.monotonic() + timeout
    query = f"""SELECT json_build_object('run_id',d.run_id,'state',d.state,
        'tasks',(SELECT json_agg(json_build_object('task_id',t.task_id,'state',t.state,
            'try_number',t.try_number) ORDER BY t.task_id)
            FROM task_instance t WHERE t.dag_id=d.dag_id AND t.run_id=d.run_id))
        FROM dag_run d WHERE d.dag_id={sql_literal(DAG_ID)} AND d.run_id={sql_literal(run_id)};"""
    previous = None
    while True:
        result = json.loads(metadata(query))
        if result != previous:
            print('DAG:', json.dumps(result), flush=True)
            previous = result
        if result['state'] == 'success':
            if len(result['tasks'] or []) != 4 or any(t['state'] != 'success' for t in result['tasks']):
                raise RuntimeError('DAG succeeded without all four expected tasks succeeding')
            return result
        if result['state'] == 'failed':
            raise RuntimeError(f'DAG run {run_id} failed; inspect its task logs in Airflow')
        if time.monotonic() >= deadline:
            raise RuntimeError(f'Timed out waiting for {run_id}; the run was left intact in Airflow')
        time.sleep(5)


def run_artifacts(run_id: str) -> dict:
    collector = '''
from collections import Counter
import hashlib, json
from pathlib import Path
import sys
base=Path('/artifacts')/hashlib.sha256(sys.argv[1].encode()).hexdigest()[:20]
stages={}
for stage in ('ingest','freshness','build','reconcile'):
    reports=[(file,json.loads(file.read_text())) for file in (base/stage).glob('*/report.json')]
    if not reports: raise RuntimeError('Missing stage report: '+stage)
    file,report=max(reports,key=lambda pair:pair[1]['started_at'])
    report['artifact_path']=str(file.parent)
    if stage=='build':
        result=json.loads((file.parent/'target/run_results.json').read_text())
        report['dbt_status_counts']=dict(Counter(r['status'] for r in result['results']))
        report['full_refresh']=result['args'].get('full_refresh',False)
    if stage=='freshness':
        result=json.loads((file.parent/'target/sources.json').read_text())
        report['source_status_counts']=dict(Counter(r['status'] for r in result['results']))
    stages[stage]=report
print(json.dumps(stages))
'''
    output = command(['docker', 'run', '--rm', '--network', 'none', '--read-only',
                      '--entrypoint', 'python', '--mount',
                      'type=volume,source=laplaptech-lab-artifacts,target=/artifacts,readonly',
                      RUNTIME_IMAGE, '-c', collector, run_id])
    stages = json.loads(output)
    if any(item['status'] != 'success' or item['exit_code'] != 0 for item in stages.values()):
        raise RuntimeError('A persisted stage report did not succeed')
    if stages['build']['dbt_status_counts'].get('success') != 31 or stages['build']['dbt_status_counts'].get('pass') != 47:
        raise RuntimeError('Expected 31 successful models and 47 passing dbt data tests')
    if stages['build']['full_refresh'] is not False:
        raise RuntimeError('Expected an incremental verification run, not a full refresh')
    return stages


def verify(platform: Path, *, run_pipeline: bool, wait_seconds: int, report: dict) -> None:
    def record(key: str, value) -> None:
        report['checks'][key] = value
        print(f'{key}: PASS', flush=True)

    if command(['docker', 'info', '--format', '{{.OSType}}']) != 'linux':
        raise RuntimeError('Start Docker Desktop with Linux containers')
    compose = ['docker', 'compose', '--project-directory', str(platform), '--env-file',
               str(platform/'.env'), '-f', str(platform/'compose.yaml'),
               '-f', str(platform/'compose.laplaptech.yaml')]
    command([*compose, 'config', '--quiet'])
    record('compose_config', True)
    report['services'] = wait_healthy()
    record('six_services_healthy', True)
    if metadata('SELECT count(*) FROM import_error;') != '0':
        raise RuntimeError('Airflow has DAG import errors')
    record('dag_import_errors', 0)
    if metadata("SELECT slots FROM slot_pool WHERE pool='laplaptech_warehouse';") != '1':
        raise RuntimeError('Expected laplaptech_warehouse pool with one slot')
    record('warehouse_pool_slots', 1)
    dag_state = metadata(f"SELECT json_build_object('is_paused',is_paused,'next_run_utc',next_dagrun_create_after) FROM dag WHERE dag_id={sql_literal(DAG_ID)};")
    if not dag_state:
        raise RuntimeError('LaplapTech DAG has not appeared in Airflow')
    record('schedule', {**json.loads(dag_state), 'timezone': 'Asia/Ho_Chi_Minh',
                        'cron': ['15 8 * * *', '45 16 * * *']})
    if metadata(f"SELECT count(*) FROM dag_run WHERE dag_id={sql_literal(DAG_ID)} AND state IN ('running','queued');") != '0':
        raise RuntimeError('A run is active; repeat verification after it finishes')

    if run_pipeline:
        if json.loads(dag_state)['is_paused']:
            raise RuntimeError('Unpause laplaptech_pipeline before requesting a real pipeline run')
        run_id = 'system_check_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        # Starting from raw watermark is intentional: this verifies normal scheduled behavior.
        command(['docker', 'exec', container('airflow-scheduler'), 'airflow', 'dags',
                 'trigger', '--run-id', run_id, '--conf', '{"full_refresh":false}', DAG_ID])
        report['pipeline_run'] = wait_run(run_id, wait_seconds)
        report['pipeline_run']['stages'] = run_artifacts(run_id)
        record('incremental_pipeline_and_dbt_tests', True)

    probe = '''
import importlib.metadata as metadata
import hashlib,json,os
from pathlib import Path
from scripts.run_airflow_stage import lab_environment,verify_database,reconcile_pipeline
env=lab_environment(); verify_database(env)
locked=dict(line.split('==',1) for line in Path('/app/requirements.lock.txt').read_text().splitlines() if '==' in line and not line.startswith('#'))
assert all(metadata.version(name)==version for name,version in locked.items())
assert os.getuid()==10001 and not Path('/app/.env').exists()
files=['/app/scripts/run_airflow_stage.py','/app/requirements.lock.txt']
print(json.dumps({'reconciliation':reconcile_pipeline(env),'locked_packages':len(locked),'uid':os.getuid(),
    'source_hashes':{f:hashlib.sha256(Path(f).read_bytes()).hexdigest() for f in files}}))
'''
    result = json.loads(command(['docker', 'run', '--rm', '--read-only', '--network',
        'laplaptech-lab', '--entrypoint', 'python', '--mount',
        'type=volume,source=laplaptech-lab-config,target=/run/laplaptech,readonly',
        RUNTIME_IMAGE, '-c', probe]))
    for source in ('scripts/run_airflow_stage.py', 'requirements.lock.txt'):
        if hashlib.sha256((PROJECT/source).read_bytes()).hexdigest() != result['source_hashes']['/app/'+source]:
            raise RuntimeError(f'Rebuild runtime image; {source} differs from the repository')
    record('runtime_identity_lock_and_reconciliation', result)

    dashboard = json.loads(command(['docker', 'inspect', container('dashboard')]))[0]
    if any(value.startswith('CLICKHOUSE_') for value in dashboard['Config']['Env']):
        raise RuntimeError('Dashboard should not receive source ClickHouse credentials')
    record('dashboard_no_source_credentials', True)
    command(['docker', 'exec', container('dashboard'), 'python', '-m', 'streamlit_app.health'])
    for service, internal_port, path in [('airflow-api-server',8080,'/api/v2/monitor/health'),
                                         ('dashboard',8501,'/_stcore/health')]:
        bindings = json.loads(command(['docker', 'inspect', container(service)]))[0]['NetworkSettings']['Ports'][f'{internal_port}/tcp']
        binding = next(item for item in bindings if item['HostIp']=='127.0.0.1')
        with urlopen(f"http://127.0.0.1:{binding['HostPort']}{path}", timeout=10) as response:
            if response.status != 200:
                raise RuntimeError(f'{service} HTTP health failed')
    record('http_and_dashboard_database_health', True)
    app_test = '''
import json
from streamlit.testing.v1 import AppTest
app=AppTest.from_file('/app/streamlit_app/app.py').run(timeout=90)
assert not app.exception and len(app.tabs)==5
for period in ('Today','All available'):
    app.radio[0].set_value(period).run(timeout=60)
    assert not app.exception
app.text_input[0].set_value('[').run(timeout=60)
assert not app.exception
print(json.dumps({'tabs':len(app.tabs),'today_and_all_available':True,'literal_search':True,'exceptions':0}))
'''
    record('dashboard_app_test_live_gold', json.loads(command(
        ['docker','exec','-i',container('dashboard'),'python','-'], stdin=app_test,
        timeout=240).splitlines()[-1]))
    record('runtime_dependencies', command(['docker','run','--rm','--network','none',
        '--entrypoint','pip',RUNTIME_IMAGE,'check']))
    for service in ('airflow-scheduler','dashboard'):
        record(service+'_dependencies', command(['docker','exec',container(service),'pip','check']))
    report['services'] = wait_healthy()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--platform-dir', type=Path, default=PROJECT.parent/'airflow-platform')
    parser.add_argument('--run-pipeline', action='store_true', help='Trigger real ingestion/freshness/build/reconcile')
    parser.add_argument('--wait-seconds', type=int, default=7200)
    parser.add_argument('--output', type=Path, default=PROJECT/'scratch/system-check.json')
    args = parser.parse_args()
    if args.wait_seconds <= 0:
        parser.error('--wait-seconds must be positive')
    report = {'started_at_utc':datetime.now(timezone.utc).isoformat(),
              'scope':'local Docker services, warehouse, Airflow and dashboard',
              'pipeline_requested':args.run_pipeline, 'checks':{}, 'status':'failed'}
    try:
        verify(args.platform_dir.resolve(), run_pipeline=args.run_pipeline,
               wait_seconds=args.wait_seconds, report=report)
        report['status'] = 'success'
        return 0
    except Exception as exc:
        report['error'] = str(exc)
        print('Verification failed:', exc, file=sys.stderr)
        return 1
    finally:
        report['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
        print(f'Report: {args.output.resolve()}',flush=True)


if __name__ == '__main__':
    raise SystemExit(main())
