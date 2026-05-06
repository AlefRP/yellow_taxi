"""Testes do callback de falha — não pode lançar exceção."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from include.callbacks import on_failure


def test_on_failure_logs_error(caplog):
    dag = MagicMock(dag_id="my_dag")
    ti = MagicMock(task_id="my_task")
    ctx = {
        "dag": dag,
        "task_instance": ti,
        "run_id": "run-1",
        "exception": RuntimeError("boom"),
    }

    with caplog.at_level(logging.ERROR, logger="include.callbacks"):
        on_failure(ctx)

    assert any("DAG failure" in r.message for r in caplog.records)


def test_on_failure_handles_partial_context():
    on_failure({})  # não pode levantar mesmo com contexto vazio
