"""Testes de include.config — precedência env > default e parsing de listas."""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def reload_config(monkeypatch):
    """Recarrega o módulo a cada teste para refletir env mudada."""

    def _reload():
        import include.config as cfg

        return importlib.reload(cfg)

    return _reload


def test_load_gcp_config_uses_env(monkeypatch, reload_config):
    monkeypatch.setenv("GCP_PROJECT_ID", "my-proj")
    monkeypatch.setenv("GCP_REGION", "europe-west1")
    monkeypatch.setenv("BQ_DATASET", "my_ds")
    monkeypatch.setenv("BQ_RAW_TABLE", "my_tbl")
    monkeypatch.setenv("BQ_LOCATION", "EU")
    monkeypatch.setenv("GCS_BUCKET", "my-bucket")
    monkeypatch.setenv("GCS_RAW_PREFIX", "my/prefix")
    monkeypatch.setenv("GCS_BRONZE_PREFIX", "br/yellow_taxi")

    cfg = reload_config().load_gcp_config()

    assert cfg.project_id == "my-proj"
    assert cfg.region == "europe-west1"
    assert cfg.bq_dataset == "my_ds"
    assert cfg.bq_raw_table == "my_tbl"
    assert cfg.bq_location == "EU"
    assert cfg.gcs_bucket == "my-bucket"
    assert cfg.gcs_raw_prefix == "my/prefix"
    assert cfg.gcs_bronze_prefix == "br/yellow_taxi"


def test_load_cloud_run_config_uses_env(monkeypatch, reload_config):
    monkeypatch.setenv("CLOUD_RUN_REGION", "southamerica-east1")
    monkeypatch.setenv("CLOUD_RUN_JOB_NAME", "yt-job")
    monkeypatch.setenv("GCP_PROJECT_ID", "my-proj")

    cfg = reload_config().load_cloud_run_config()

    assert cfg.project_id == "my-proj"
    assert cfg.region == "southamerica-east1"
    assert cfg.job_name == "yt-job"


def test_load_cloud_run_config_falls_back_to_gcp_region(monkeypatch, reload_config):
    monkeypatch.delenv("CLOUD_RUN_REGION", raising=False)
    monkeypatch.setenv("GCP_REGION", "us-east4")

    cfg = reload_config().load_cloud_run_config()

    assert cfg.region == "us-east4"


def test_load_gcp_config_falls_back_to_defaults(monkeypatch, reload_config):
    for v in [
        "GCP_PROJECT_ID",
        "BQ_DATASET",
        "BQ_RAW_TABLE",
        "BQ_LOCATION",
        "GCS_BUCKET",
        "GCS_RAW_PREFIX",
    ]:
        monkeypatch.delenv(v, raising=False)

    cfg = reload_config().load_gcp_config()

    assert cfg.project_id == "dbtbigq-450122"
    assert cfg.bq_dataset == "taxis_ny"
    assert cfg.bq_raw_table == "yellow_taxi"


def test_alert_emails_parses_comma_separated(monkeypatch, reload_config):
    monkeypatch.setenv("ALERT_EMAILS", "a@x.com, b@x.com ,c@x.com")
    assert reload_config().alert_emails() == ["a@x.com", "b@x.com", "c@x.com"]


def test_alert_emails_empty_returns_empty_list(monkeypatch, reload_config):
    monkeypatch.delenv("ALERT_EMAILS", raising=False)
    assert reload_config().alert_emails() == []


def test_gcp_config_is_immutable():
    from include.config import load_gcp_config

    cfg = load_gcp_config()
    with pytest.raises(Exception):
        cfg.project_id = "outro"  # frozen dataclass
