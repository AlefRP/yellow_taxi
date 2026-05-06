"""Testes do builder de default_args padronizado."""

from __future__ import annotations

from datetime import timedelta

from include.callbacks import on_failure
from include.default_args import build_default_args


def test_default_args_has_required_keys():
    args = build_default_args()
    for key in (
        "owner",
        "depends_on_past",
        "email",
        "email_on_failure",
        "email_on_retry",
        "retries",
        "retry_delay",
        "on_failure_callback",
    ):
        assert key in args, f"chave obrigatória ausente: {key}"


def test_default_args_retries_at_least_two():
    assert build_default_args()["retries"] >= 2


def test_default_args_retry_delay_is_timedelta():
    assert isinstance(build_default_args()["retry_delay"], timedelta)


def test_default_args_callback_is_on_failure():
    assert build_default_args()["on_failure_callback"] is on_failure


def test_default_args_owner_override():
    assert build_default_args(owner="alice")["owner"] == "alice"


def test_default_args_email_disabled_when_no_alerts(monkeypatch):
    monkeypatch.delenv("ALERT_EMAILS", raising=False)
    args = build_default_args()
    assert args["email"] == []
    assert args["email_on_failure"] is False
