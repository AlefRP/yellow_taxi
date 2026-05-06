"""
Caminhos e constantes usados pelos DAGs.
"""

import os
from pathlib import Path

from cosmos.config import ExecutionConfig

# Em produção (Astro container) AIRFLOW_HOME=/usr/local/airflow.
# Em CI/pytest local apontamos para a raiz do repo via conftest.py.
AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))

DBT_PROJECT_DIR = AIRFLOW_HOME / "dbt" / "yellow_taxi"
DBT_EXECUTABLE = AIRFLOW_HOME / "dbt_venv" / "bin" / "dbt"

venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(DBT_EXECUTABLE),
)
