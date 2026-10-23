"""Docker task operator shared by projects; policy is mounted locally, never fetched."""
from airflow.providers.docker.operators.docker import DockerOperator
from platform_resource_policy import QuotaDockerClient, load_policy, task_budget


class BudgetDockerOperator(DockerOperator):
    def __init__(self, *, resource_project, resource_stage, **kwargs):
        budget = task_budget(load_policy(), resource_project, resource_stage)
        for key in ('mem_limit', 'cpus', 'pool', 'pool_slots'):
            if key in kwargs:
                raise ValueError(f'{key} is managed by resources.json')
        self.budget_cpus, self.budget_memory_mib = budget['cpus'], budget['memory_mib']
        environment = {**kwargs.pop('environment', {}), 'DBT_THREADS': str(budget['dbt_threads']),
                       'OMP_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1'}
        labels = {**kwargs.pop('labels', {}), 'com.airflow.resource-policy': 'v1',
                  'com.airflow.project': resource_project, 'com.airflow.stage': resource_stage}
        super().__init__(mem_limit=f'{budget["memory_mib"]}m', cpus=budget['cpus'],
                         pool=budget['pool'], pool_slots=budget['pool_slots'],
                         environment=environment, labels=labels, **kwargs)

    @property
    def cli(self):
        return QuotaDockerClient(super().cli, self.budget_cpus, self.budget_memory_mib)
