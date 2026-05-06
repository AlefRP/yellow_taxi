"""
Caminhos e constantes usados pelos DAGs.
"""

from pathlib import Path

from cosmos.config import ExecutionConfig

AIRFLOW_HOME = Path("/usr/local/airflow")

DBT_PROJECT_DIR = AIRFLOW_HOME / "dbt" / "yellow_taxi"
DBT_EXECUTABLE = AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(DBT_EXECUTABLE),
)
