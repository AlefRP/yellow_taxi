"""
Executa o projeto dbt (yellow_taxi) no BigQuery via Astronomer Cosmos.

- Schedule diário, sem catchup.
- `full_refresh` é desligado por padrão; reexecutar manualmente com
  `--conf '{"full_refresh": true}'` quando necessário.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ProjectConfig

from include.constants import DBT_PROJECT_DIR, venv_execution_config
from include.default_args import build_default_args
from include.profiles import bq_profile


@dag(
    dag_id="yellow_taxi",
    description="Roda o projeto dbt yellow_taxi no BigQuery via Cosmos.",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=build_default_args(owner="alef_rp"),
    tags=["dbt", "bigquery", "yellow_taxi"],
)
def yellow_taxi_dag():
    start = EmptyOperator(task_id="start")

    dbt_tasks = DbtTaskGroup(
        group_id="dbt_build",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=bq_profile,
        execution_config=venv_execution_config,
        operator_args={"install_deps": True},
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_tasks >> end


yellow_taxi_dag()
