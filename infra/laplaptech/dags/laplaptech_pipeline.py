"""Local ClickHouse -> PostgreSQL -> Gold pipeline, twice daily in Vietnam time."""
from datetime import timedelta
from hashlib import sha256
from pathlib import Path

import pendulum
from airflow.sdk import Asset, DAG, Param
from budget_docker import BudgetDockerOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from docker.types import Mount

RAW = Asset(name='LaplapTech raw', uri='x-laplaptech://local/raw')
GOLD = Asset(name='LaplapTech validated Gold', uri='x-laplaptech://local/gold')
RELEASE_DIR = Path(__file__).with_name('.cosmos')


def pipeline_stage(stage: str, minutes: int, *, retries: int = 1, task_id=None,
                   image='laplaptech-runtime:local-v1', command=None, environment=None,
                   **kwargs) -> BudgetDockerOperator:
    if command is None:
        command = ([stage] if stage != 'build' else
                   "{{ ['build', '--full-refresh'] if params.full_refresh else ['build'] }}")
    return BudgetDockerOperator(
        resource_project='laplaptech', resource_stage=stage,
        task_id=task_id or stage,
        image=image,
        command=command,
        docker_url='unix://var/run/docker.sock', api_version='auto',
        network_mode='laplaptech-lab', mount_tmp_dir=False,
        mounts=[
            Mount(source='laplaptech-lab-config', target='/run/laplaptech', type='volume', read_only=True),
            Mount(source='laplaptech-lab-artifacts', target='/artifacts', type='volume'),
        ],
        environment={'PIPELINE_RUN_ID': '{{ run_id }}', 'PIPELINE_ATTEMPT': '{{ ti.try_number }}',
                     **(environment or {})},
        labels={'com.laplaptech.project': 'laplaptech', 'com.laplaptech.environment': 'local'},
        user='10001:10001', force_pull=False,
        auto_remove='force', do_xcom_push=False,
        retries=retries, retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=minutes),
        log_opts_max_size='10m', log_opts_max_file='3',
        **kwargs,
    )


def cosmos_build(freshness, reconcile, image: str, manifest: Path, release: str) -> None:
    from cosmos import DbtTaskGroup, ExecutionConfig, ProjectConfig, RenderConfig
    from cosmos.constants import DbtResourceType, ExecutionMode, LoadMode, TestBehavior

    def dbt_task(task_id, command, *, retries=1, **kwargs):
        return pipeline_stage(
            'build', 30, task_id=task_id, image=image, command=command,
            entrypoint=['python', '-m', 'scripts.run_cosmos'], retries=retries,
            environment={'LAPLAPTECH_MANIFEST_SHA256': release, 'PIPELINE_TASK_ID': '{{ ti.task_id }}'},
            **kwargs,
        )

    def convert_model(*, dag, task_group, task_id, node, **kwargs):
        command = ("{{ ['model', '--model', " + repr(node.unique_id) +
                   "] + (['--full-refresh'] if params.full_refresh else []) }}")
        return dbt_task(task_id, command, dag=dag, task_group=task_group)

    models = DbtTaskGroup(
        group_id='transform',
        project_config=ProjectConfig(manifest_path=manifest, project_name='laplaptech_pipeline',
                                     install_dbt_deps=False),
        execution_config=ExecutionConfig(execution_mode=ExecutionMode.DOCKER, dbt_project_path=Path('/app/dbt')),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST, test_behavior=TestBehavior.NONE,
            emit_datasets=False, node_conversion_by_task_group=False,
            node_converters={DbtResourceType.MODEL: convert_model},
        ),
    )
    tests = dbt_task('dbt_tests', ['test'], retries=0)
    collect = dbt_task('collect_dbt_results', ['collect'], retries=0)
    freshness >> models >> tests >> collect >> reconcile


def make_dag(dag_id: str, *, use_cosmos: bool, trial: bool = False):
    schedule = None if trial else MultipleCronTriggerTimetable(
        '0 7 * * *', '0 19 * * *', timezone='Asia/Ho_Chi_Minh', run_immediately=False,
    )
    image = 'laplaptech-runtime:local-v1'
    if use_cosmos:
        manifest = RELEASE_DIR / 'manifest.json'
        release = sha256(manifest.read_bytes()).hexdigest()
        image = f'laplaptech-runtime:cosmos-{release[:32]}'
    with DAG(
        dag_id=dag_id,
        description='Public ClickHouse -> local PostgreSQL -> tested and reconciled Gold',
        start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Ho_Chi_Minh'),
        schedule=schedule, catchup=False, is_paused_upon_creation=True,
        max_active_runs=1, max_active_tasks=1,
        dagrun_timeout=timedelta(hours=2), render_template_as_native_obj=True,
        params={'full_refresh': Param(False, type='boolean', description='Rebuild incremental dbt models')},
        tags=['laplaptech', 'local'] + (['cosmos'] if use_cosmos else []),
        auto_register=False,
    ) as pipeline:
        # The manual trial must not announce that production assets were published.
        ingest = pipeline_stage('ingest', 45, image=image, outlets=[] if trial else [RAW])
        freshness = pipeline_stage('freshness', 5, image=image, retries=0, inlets=[] if trial else [RAW])
        reconcile = pipeline_stage('reconcile', 10, image=image, retries=0, outlets=[] if trial else [GOLD])
        ingest >> freshness
        if use_cosmos:
            cosmos_build(freshness, reconcile, image, manifest, release)
        else:
            freshness >> pipeline_stage('build', 30, inlets=[RAW]) >> reconcile
    return pipeline


mode_path = RELEASE_DIR / 'mode'
mode = mode_path.read_text(encoding='utf-8').strip() if mode_path.is_file() else 'legacy'
if mode not in {'legacy', 'trial', 'cosmos'}:
    raise ValueError('Invalid LapLapTech deployment mode')
dag = make_dag('laplaptech_pipeline', use_cosmos=mode == 'cosmos')
if mode == 'trial':
    trial_dag = make_dag('laplaptech_cosmos_trial', use_cosmos=True, trial=True)
