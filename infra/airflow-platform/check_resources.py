"""Read-only audit of the shared resource policy, containers, pool and live usage."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent/'dags'))
from platform_resource_policy import load_policy, validate_policy


def docker(*args):
    result = subprocess.run(['docker', *args], text=True, capture_output=True, timeout=45)
    if result.returncode:
        # Never print inspect stdout: it can contain env credentials.
        raise RuntimeError(result.stderr.strip())
    return result.stdout.strip()


def audit(platform):
    policy = load_policy(platform/'resources.json')
    budget = validate_policy(policy)
    info = json.loads(docker('info', '--format', '{{json .}}'))
    errors = []
    if info['MemTotal'] // 1024**2 < budget['engine_required_mib']:
        errors.append('Engine RAM is below the planned services + tasks + headroom budget')
    targets = [(name, 'airflow-platform', name, value)
               for name, value in policy['services'].items() if not value.get('transient')]
    for project in policy['projects'].values():
        for service in project['services'].values():
            targets.append((service['compose_service'], 'airflow-platform',
                            service['compose_service'], service))
    for name, service in policy.get('external_services', {}).items():
        targets.append((name, service['compose_project'], service['compose_service'], service))
    containers = []
    optional_running_mib = 0
    for service, compose_project, compose_service, limits in targets:
        ids = docker('ps', '-a', '--filter', f'label=com.docker.compose.project={compose_project}',
                     '--filter', f'label=com.docker.compose.service={compose_service}',
                     '--format', '{{.ID}}').splitlines()
        if not ids and limits.get('optional'):
            continue
        if len(ids) != 1:
            errors.append(f'{service}: expected one managed container, got {len(ids)}')
            continue
        item = json.loads(docker('inspect', ids[0]))[0]
        config, state = item['HostConfig'], item['State']
        running = state['Status'] == 'running'
        if limits.get('optional') and running:
            optional_running_mib += limits['memory_mib']
        snapshot = {'service': service, 'container_id': ids[0],
                    'compose_project': compose_project,
                    'memory_mib': config['Memory']//1024**2,
                    'cpu_limit': config['NanoCpus']/1e9, 'pids_limit': config['PidsLimit'],
                    'swap_disabled': config['MemorySwap'] == config['Memory'],
                    'oom_killed': state['OOMKilled'], 'restarts': item['RestartCount'],
                    'state': state['Status'], 'health': state.get('Health', {}).get('Status')}
        if snapshot['memory_mib'] != limits['memory_mib'] or snapshot['cpu_limit'] != limits['cpus']:
            errors.append(service + ': effective limits differ from policy; configure and recreate')
        if not snapshot['swap_disabled'] or snapshot['pids_limit'] <= 0:
            errors.append(service + ': swap/PID guardrails differ from policy')
        if snapshot['oom_killed'] or (not limits.get('optional') and not running) or (running and snapshot['health'] != 'healthy'):
            errors.append(service + ': not healthy, or last exit was OOM')
        containers.append(snapshot)
    effective_required_mib = budget['required_host_mib'] + optional_running_mib
    if effective_required_mib > policy['host']['memory_mib']:
        errors.append(f'Running optional services raise the host budget to {effective_required_mib} MiB '
                      f'over {policy["host"]["memory_mib"]} MiB')
    pool = policy['airflow']['pool']
    if not pool.replace('_', '').isalnum(): raise ValueError('Pool name must be alphanumeric/underscore')
    actual = docker('exec', 'airflow-platform-metadata-1', 'psql', '-U', 'airflow', '-d', 'airflow_meta', '-Atc', f"SELECT slots FROM slot_pool WHERE pool='{pool}';")
    if actual != str(policy['airflow']['pool_slots']): errors.append('Shared pool capacity differs from resource policy')
    running_ids = [c['container_id'] for c in containers if c['state'] == 'running']
    usage = docker('stats', '--no-stream', '--format', '{{json .}}', *running_ids) if running_ids else ''
    return {'checked_at_utc': datetime.now(timezone.utc).isoformat(), 'status': 'failed' if errors else 'success',
            'engine': {'memory_mib': info['MemTotal']//1024**2, 'cpus': info['NCPU']},
            'budget': {**budget, 'optional_running_mib': optional_running_mib,
                       'effective_required_host_mib': effective_required_mib},
            'pool': {'name': pool, 'slots': actual}, 'services': containers,
            'usage': [json.loads(line) for line in usage.splitlines()], 'errors': errors}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--platform-dir', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = audit(args.platform_dir)
    print(json.dumps(result, indent=2))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    raise SystemExit(result['status'] != 'success')
