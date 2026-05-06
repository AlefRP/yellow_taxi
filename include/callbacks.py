"""
Callbacks reutilizáveis para DAGs.

Mantém um único lugar para evoluir alertas (Slack, PagerDuty, etc.)
sem precisar editar cada DAG.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def on_failure(context: dict) -> None:
    """Log estruturado em caso de falha. Plugar Slack/PagerDuty aqui no futuro."""
    ti = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "?"
    task_id = ti.task_id if ti else "?"
    run_id = context.get("run_id", "?")
    exc = context.get("exception")

    log.error(
        "DAG failure",
        extra={
            "dag_id": dag_id,
            "task_id": task_id,
            "run_id": run_id,
            "exception": str(exc) if exc else None,
        },
    )
