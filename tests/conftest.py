"""Configuração comum dos testes pytest."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Permite `from include.* import ...` sem instalar o projeto.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# AIRFLOW_HOME aponta para o repo (DagBag procura DAGs em $AIRFLOW_HOME/dags).
os.environ.setdefault("AIRFLOW_HOME", str(ROOT))
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(ROOT / "dags"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
