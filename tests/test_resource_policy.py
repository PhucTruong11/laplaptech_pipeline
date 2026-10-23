"""Admission budgets and actual Docker HostConfig constraints, without a daemon."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('policy_for_test', ROOT/'infra/airflow-platform/dags/platform_resource_policy.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ResourcePolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = json.loads((ROOT/'infra/airflow-platform/resources.json').read_text())

    def test_all_admitted_task_pairs_fit_shared_memory_budget(self):
        tasks = [module.task_budget(self.policy, 'laplaptech', stage) for stage in self.policy['projects']['laplaptech']['tasks']]
        for a in tasks:
            for b in tasks:
                if a['pool_slots'] + b['pool_slots'] <= 4:
                    self.assertLessEqual(a['memory_mib'] + b['memory_mib'], 2048)
        self.assertEqual(module.task_budget(self.policy, 'laplaptech', 'ingest')['pool_slots'], 4)

    def test_new_projects_must_fit_idle_and_task_budgets(self):
        future = deepcopy(self.policy['projects']['laplaptech'])
        for service in future['services'].values():
            service['compose_service'] += '-future'
        self.policy['projects']['future'] = future
        with self.assertRaisesRegex(ValueError, 'exceeds WSL'):
            module.validate_policy(self.policy)
        future['services'] = {}
        module.validate_policy(self.policy)  # Reusing an existing DB is allowed.

    def test_external_database_and_optional_dashboard_fit_one_at_a_time(self):
        self.policy['external_services'] = {
            'brewlite-db': {'memory_mib': 512, 'cpus': 0.5,
                            'compose_project': 'docker', 'compose_service': 'db',
                            'optional': True}
        }
        budget = module.validate_policy(self.policy)
        self.assertEqual(budget['required_host_mib'], 7424)
        # Starting either service adds 512 MiB; starting both exceeds 8 GiB.
        self.assertEqual(budget['required_host_mib'] + 512, 7936)
        self.assertGreater(budget['required_host_mib'] + 1024, self.policy['host']['memory_mib'])

    def test_external_service_without_identity_or_valid_limits_is_rejected(self):
        for service in ({'memory_mib': 512, 'cpus': 0.5, 'compose_project': 'docker'},
                        {'memory_mib': 0, 'cpus': 0.5,
                         'compose_project': 'docker', 'compose_service': 'db'}):
            policy = deepcopy(self.policy)
            policy['external_services'] = {'brewlite-db': service}
            with self.assertRaises(ValueError):
                module.validate_policy(policy)

    def test_oversized_task_cannot_sit_forever_in_pool(self):
        self.policy['projects']['laplaptech']['tasks']['ingest']['memory_mib'] = 2049
        with self.assertRaisesRegex(ValueError, 'cannot fit'):
            module.validate_policy(self.policy)

    def test_invalid_numeric_limits_are_rejected(self):
        for invalid in (0, -1, True, float('nan'), float('inf')):
            policy = deepcopy(self.policy)
            policy['services']['metadata']['cpus'] = invalid
            with self.assertRaises(ValueError):
                module.validate_policy(policy)

    def test_cpu_quota_and_no_swap_are_applied_before_container_creation(self):
        class Client:
            def create_host_config(self, **kwargs): return kwargs
            def ping(self): return True
        proxy = module.QuotaDockerClient(Client(), 1.5, 512)
        result = proxy.create_host_config(cpu_shares=1024, network_mode='test', mem_limit='wrong')
        self.assertEqual(result['nano_cpus'], 1_500_000_000)
        self.assertEqual(result['mem_limit'], 512 * 1024**2)
        self.assertEqual(result['memswap_limit'], result['mem_limit'])
        self.assertEqual(result['network_mode'], 'test')
        self.assertEqual(result['pids_limit'], 256)
        self.assertTrue(proxy.ping())

    def test_missing_project_never_inherits_another_projects_budget(self):
        with self.assertRaises(KeyError):
            module.task_budget(self.policy, 'unregistered', 'ingest')

    def test_service_names_and_thread_bounds_fail_before_deployment(self):
        for services, threads in (({'warehouse': {'memory_mib': 256, 'cpus': 0.5}}, 2),
                                  ({'warehouse': {'memory_mib': 256, 'cpus': 0.5, 'compose_service': 'metadata'}}, 2),
                                  ({}, 17)):
            policy = deepcopy(self.policy)
            policy['projects']['future'] = {'services': services, 'tasks': {}, 'dbt_threads': threads}
            with self.assertRaises(ValueError):
                module.validate_policy(policy)
