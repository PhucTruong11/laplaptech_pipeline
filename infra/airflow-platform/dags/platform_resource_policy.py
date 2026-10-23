"""Shared, dependency-free resource contracts for every project on this platform."""
import json
import math
import os
import re
from pathlib import Path


def positive_integer(value, label):
    if type(value) is not int or value <= 0:
        raise ValueError(f'{label} must be a positive integer')
    return value


def limits(item, label):
    positive_integer(item['memory_mib'], label + '.memory_mib')
    cpu = item['cpus']
    if type(cpu) not in (int, float) or not math.isfinite(cpu) or cpu <= 0:
        raise ValueError(label + '.cpus must be finite and positive')


def validate_policy(policy):
    if policy['version'] != 1:
        raise ValueError('Unsupported resource policy version')
    host, airflow = policy['host'], policy['airflow']
    for key in ('memory_mib', 'processors', 'swap_mib', 'engine_overhead_mib'):
        positive_integer(host[key], 'host.' + key)
    for key in ('parallelism', 'max_active_tasks_per_dag', 'pool_slots', 'slot_mib'):
        positive_integer(airflow[key], 'airflow.' + key)
    if not isinstance(airflow['pool'], str) or not re.fullmatch(r'[A-Za-z0-9_]+', airflow['pool']):
        raise ValueError('Shared pool name must use letters, digits and underscores')
    steady = 0
    service_names = set(policy['services'])
    for name, service in policy['services'].items():
        limits(service, name)
        if not service.get('transient', False) and not service.get('optional', False):
            steady += service['memory_mib']
    for project, config in policy['projects'].items():
        if not re.fullmatch(r'[a-z][a-z0-9_]*', project):
            raise ValueError('Project names must use lowercase letters, digits and underscores')
        positive_integer(config['dbt_threads'], project + '.dbt_threads')
        if config['dbt_threads'] > 16:
            raise ValueError('dbt_threads must be between 1 and 16')
        for name, service in config['services'].items():
            limits(service, project + '.' + name)
            compose_name = service.get('compose_service', '')
            if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]*', compose_name) or compose_name in service_names:
                raise ValueError('Every project service needs a unique compose_service name')
            service_names.add(compose_name)
            if not service.get('optional', False):
                steady += service['memory_mib']
        for name, task in config['tasks'].items():
            limits(task, project + '.' + name)
            if math.ceil(task['memory_mib'] / airflow['slot_mib']) > airflow['pool_slots']:
                raise ValueError(f'{project}.{name} cannot fit in the shared pool')
    for name, service in policy.get('external_services', {}).items():
        limits(service, 'external_services.' + name)
        if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]*', service.get('compose_project', '')):
            raise ValueError(f'{name}: external service needs a valid compose_project')
        compose_name = service.get('compose_service', '')
        if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]*', compose_name):
            raise ValueError(f'{name}: external service needs a valid compose_service')
        if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]*', name):
            raise ValueError('External service names must be Docker-compatible')
        if not service.get('optional', False):
            steady += service['memory_mib']
    task_budget = airflow['pool_slots'] * airflow['slot_mib']
    headroom = positive_integer(policy['headroom_mib'], 'headroom_mib')
    required = steady + task_budget + headroom + host['engine_overhead_mib']
    if required > host['memory_mib']:
        raise ValueError(f'Resource budget {required} MiB exceeds WSL {host["memory_mib"]} MiB')
    return {'services_mib': steady, 'tasks_mib': task_budget, 'headroom_mib': headroom,
            'required_host_mib': required, 'engine_required_mib': required - host['engine_overhead_mib']}


def load_policy(path=None):
    policy = json.loads(Path(path or os.environ.get('AIRFLOW_RESOURCE_POLICY', '/opt/airflow/resources.json')).read_text())
    validate_policy(policy)
    return policy


def task_budget(policy, project, stage):
    validate_policy(policy)
    config = policy['projects'][project]
    task = config['tasks'][stage]
    return {**task, 'pool': policy['airflow']['pool'],
            'pool_slots': math.ceil(task['memory_mib'] / policy['airflow']['slot_mib']),
            'dbt_threads': config['dbt_threads']}


class QuotaDockerClient:
    """Keep DockerOperator behavior, adding actual CFS quota rather than CPU shares."""
    def __init__(self, client, cpus, memory_mib):
        self.client, self.cpus, self.memory_mib = client, cpus, memory_mib

    def __getattr__(self, name):
        return getattr(self.client, name)

    def create_host_config(self, *args, **kwargs):
        kwargs.update(nano_cpus=int(self.cpus * 1_000_000_000),
                      mem_limit=self.memory_mib * 1024**2,
                      memswap_limit=self.memory_mib * 1024**2, pids_limit=256)
        return self.client.create_host_config(*args, **kwargs)
