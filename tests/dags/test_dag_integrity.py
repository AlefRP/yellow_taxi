"""
Testes de integridade dos DAGs:
- Importação sem erros
- Presença de tags, owner, description
- Retries >= 2
- Sem ciclos
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager

import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace: str):
    logger = logging.getLogger(namespace)
    old = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old


def _strip_prefix(path: str) -> str:
    return os.path.relpath(path, os.environ.get("AIRFLOW_HOME", "/usr/local/airflow"))


def _dag_bag() -> DagBag:
    with suppress_logging("airflow"):
        return DagBag(include_examples=False)


_BAG = _dag_bag()
_DAGS = list(_BAG.dags.items())
_IDS = [dag_id for dag_id, _ in _DAGS]


def test_no_import_errors():
    errors = {_strip_prefix(k): v.strip() for k, v in _BAG.import_errors.items()}
    assert not errors, f"Erros de import: {errors}"


@pytest.mark.parametrize("dag_id,dag", _DAGS, ids=_IDS)
def test_dag_has_tags(dag_id, dag):
    assert dag.tags, f"{dag_id} sem tags"


@pytest.mark.parametrize("dag_id,dag", _DAGS, ids=_IDS)
def test_dag_has_description(dag_id, dag):
    assert dag.description, f"{dag_id} sem description"


@pytest.mark.parametrize("dag_id,dag", _DAGS, ids=_IDS)
def test_dag_has_owner(dag_id, dag):
    owner = dag.default_args.get("owner") or dag.owner
    assert owner and owner != "airflow", f"{dag_id} sem owner customizado"


@pytest.mark.parametrize("dag_id,dag", _DAGS, ids=_IDS)
def test_dag_retries(dag_id, dag):
    retries = dag.default_args.get("retries", 0)
    assert retries >= 2, f"{dag_id}: retries={retries} (esperado >= 2)"


@pytest.mark.parametrize("dag_id,dag", _DAGS, ids=_IDS)
def test_dag_no_cycles(dag_id, dag):
    # `dag.test_cycle()` lança em caso de ciclo (deprecado em alguns dialects;
    # `dag.topological_sort()` também serve como verificação implícita).
    dag.topological_sort()
