"""Local ClickHouse -> PostgreSQL -> Gold pipeline, twice daily in Vietnam time."""
from datetime import timedelta

import pendulum
from airflow.sdk import Asset, DAG, Param
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from docker.types import Mount

RAW = Asset(name='LaplapTech raw', uri='x-laplaptech://local/raw')
GOLD = Asset(name='LaplapTech validated Gold', uri='x-laplaptech://local/gold')


def pipeline_stage(stage: str, minutes: int, *, retries: int = 1, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=stage,
        image='laplaptech-runtime:local-v1',
        command=([stage] if stage != 'build' else
                 "{{ ['build', '--full-refresh'] if params.full_refresh else ['build'] }}"),
        docker_url='unix://var/run/docker.sock', api_version='auto',
        network_mode='laplaptech-lab', mount_tmp_dir=False,
        mounts=[
            Mount(source='laplaptech-lab-config', target='/run/laplaptech', type='volume', read_only=True),
            Mount(source='laplaptech-lab-artifacts', target='/artifacts', type='volume'),
        ],
        environment={'PIPELINE_RUN_ID': '{{ run_id }}', 'PIPELINE_ATTEMPT': '{{ ti.try_number }}'},
        labels={'com.laplaptech.project': 'laplaptech', 'com.laplaptech.environment': 'local'},
        user='10001:10001', mem_limit='2g', force_pull=False,
        auto_remove='force', do_xcom_push=False, pool='laplaptech_warehouse',
        retries=retries, retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=minutes),
        log_opts_max_size='10m', log_opts_max_file='3',
        **kwargs,
    )


with DAG(
    dag_id='laplaptech_pipeline',
    description='Public ClickHouse -> local PostgreSQL -> tested and reconciled Gold',
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Ho_Chi_Minh'),
    schedule=MultipleCronTriggerTimetable(
        '15 8 * * *', '45 16 * * *', timezone='Asia/Ho_Chi_Minh', run_immediately=False,
    ),
    catchup=False, is_paused_upon_creation=True,
    max_active_runs=1, max_active_tasks=1,
    dagrun_timeout=timedelta(hours=2), render_template_as_native_obj=True,
    params={'full_refresh': Param(False, type='boolean', description='Rebuild incremental dbt models for this run')},
    tags=['laplaptech', 'local'],
) as dag:
    ingest = pipeline_stage('ingest', 45, outlets=[RAW])
    freshness = pipeline_stage('freshness', 5, retries=0, inlets=[RAW])
    build = pipeline_stage('build', 30, inlets=[RAW])
    reconcile = pipeline_stage('reconcile', 10, retries=0, outlets=[GOLD])
    ingest >> freshness >> build >> reconcile
