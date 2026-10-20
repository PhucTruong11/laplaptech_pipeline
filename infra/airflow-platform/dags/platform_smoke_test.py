"""First Airflow 3 exercise: dependency, small XCom, validation, and task logs."""

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="platform_smoke_test",
    description="Manual two-task exercise; does not access project databases.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=5),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["platform", "learning"],
)
def platform_smoke_test():
    @task(execution_timeout=timedelta(minutes=1))
    def prepare_payload() -> dict:
        payload = {"project": "airflow-platform", "values": [2, 3, 5]}
        logging.info("Prepared a small example payload for the next task.")
        return payload

    @task(execution_timeout=timedelta(minutes=1))
    def validate_payload(payload: dict) -> None:
        total = sum(payload["values"])
        if payload["project"] != "airflow-platform" or total != 10:
            raise ValueError("Unexpected example payload")
        logging.info("Smoke test passed: project=%s, total=%s", payload["project"], total)

    validate_payload(prepare_payload())


platform_smoke_test()
