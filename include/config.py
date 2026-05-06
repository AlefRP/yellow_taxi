"""
Configuração centralizada do projeto.

Ordem de prioridade para cada chave:
    1. Airflow Variable (se existir)
    2. Variável de ambiente
    3. Default codificado abaixo

Mantemos um único ponto de leitura para evitar valores hardcoded espalhados
pelos DAGs e facilitar testes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _get(key_var: str, key_env: str, default: str | None = None) -> str | None:
    """Lê primeiro de Airflow Variable, depois de env, depois default."""
    try:
        from airflow.models import Variable

        value = Variable.get(key_var, default_var=None)
        if value:
            return value
    except Exception:
        # Fora de runtime do Airflow (ex.: dbt parse, pytest sem DB)
        pass
    return os.getenv(key_env, default)


@dataclass(frozen=True)
class GcpConfig:
    project_id: str
    region: str
    bq_dataset: str
    bq_raw_table: str
    bq_location: str
    gcs_bucket: str
    gcs_raw_prefix: str
    gcs_bronze_prefix: str
    bq_conn_id: str = "my_bigquery_connection"


@dataclass(frozen=True)
class CloudRunConfig:
    project_id: str
    region: str
    job_name: str


def load_gcp_config() -> GcpConfig:
    return GcpConfig(
        project_id=_get("gcp_project_id", "GCP_PROJECT_ID", "dbtbigq-450122"),
        region=_get("gcp_region", "GCP_REGION", "us-central1"),
        bq_dataset=_get("bq_dataset", "BQ_DATASET", "taxis_ny"),
        bq_raw_table=_get("bq_raw_table", "BQ_RAW_TABLE", "yellow_taxi"),
        bq_location=_get("bq_location", "BQ_LOCATION", "US"),
        gcs_bucket=_get("gcs_bucket", "GCS_BUCKET", "dbt_teste"),
        gcs_raw_prefix=_get("gcs_raw_prefix", "GCS_RAW_PREFIX", "raw/yellow_tripdata"),
        gcs_bronze_prefix=_get("gcs_bronze_prefix", "GCS_BRONZE_PREFIX", "bronze/yellow_taxi"),
    )


def load_cloud_run_config() -> CloudRunConfig:
    cfg = load_gcp_config()
    return CloudRunConfig(
        project_id=cfg.project_id,
        region=_get("cloud_run_region", "CLOUD_RUN_REGION", cfg.region),
        job_name=_get("cloud_run_job_name", "CLOUD_RUN_JOB_NAME", "yellow-taxi-ingest"),
    )


def alert_emails() -> list[str]:
    raw = _get("alert_emails", "ALERT_EMAILS", "")
    return [e.strip() for e in raw.split(",") if e.strip()]
