"""
Pipeline de ingestão raw:

    Cloud Run Job (Kaggle → GCS raw parquet + GCS bronze Iceberg)
        ↓
    BigQuery LOAD do parquet raw → tabela `raw_yellow_taxi`

A bronze Iceberg é gravada em GCS pela Cloud Run para consumo por outros
mecanismos (Spark/Trino) — o BigQuery deste pipeline lê apenas a raw.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

from include.bq_loader import build_parquet_load_config
from include.config import load_cloud_run_config, load_gcp_config
from include.default_args import build_default_args

_gcp = load_gcp_config()
_run = load_cloud_run_config()


@dag(
    dag_id="gcs_to_bigquery",
    description=(
        "Cloud Run baixa o mês do Kaggle e grava raw (parquet) + bronze "
        "(Iceberg) em GCS; depois carrega o parquet raw na tabela do BigQuery."
    ),
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args=build_default_args(owner="alef_rp"),
    tags=["ingestion", "cloud-run", "bigquery"],
)
def gcs_to_bigquery_dag():
    ingest = CloudRunExecuteJobOperator(
        task_id="ingest_kaggle_to_gcs",
        project_id=_run.project_id,
        region=_run.region,
        job_name=_run.job_name,
        gcp_conn_id=_gcp.bq_conn_id,
        deferrable=True,
        overrides={
            "container_overrides": [
                {
                    "env": [
                        {"name": "YEAR", "value": "{{ data_interval_start.year }}"},
                        {"name": "MONTH", "value": "{{ data_interval_start.month }}"},
                    ]
                }
            ]
        },
    )

    @task
    def make_load_config(data_interval_start=None) -> dict:
        return build_parquet_load_config(data_interval_start, load_gcp_config())

    bq_load = BigQueryInsertJobOperator(
        task_id="bq_load_raw",
        configuration=make_load_config(),
        gcp_conn_id=_gcp.bq_conn_id,
        location=_gcp.bq_location,
    )

    ingest >> bq_load


gcs_to_bigquery_dag()
