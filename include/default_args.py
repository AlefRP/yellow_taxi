"""
Default args padrão para DAGs do projeto.

Centralizado para garantir consistência: retries, alertas e ownership iguais
em todas as DAGs.
"""

from __future__ import annotations

from datetime import timedelta

from include.callbacks import on_failure
from include.config import alert_emails


def build_default_args(owner: str = "data-eng") -> dict:
    return {
        "owner": owner,
        "depends_on_past": False,
        "email": alert_emails(),
        "email_on_failure": bool(alert_emails()),
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_failure,
    }
