"""
Pipeline de ingestão raw → bronze:

    1. Cloud Run Job: Kaggle → GCS raw (Parquet) + GCS bronze (Iceberg)
    2. Descobre o metadata.json mais recente do Iceberg em GCS
    3. CREATE OR REPLACE EXTERNAL TABLE BigLake apontando para esse metadata

A jusante o DAG `yellow_taxi` roda dbt sobre essa external table.
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

from include.bq_iceberg import build_external_iceberg_ddl, latest_metadata_uri
from include.config import load_cloud_run_config, load_gcp_config
from include.default_args import build_default_args

_gcp = load_gcp_config()
_run = load_cloud_run_config()


@dag(
    dag_id="gcs_to_bigquery",
    description=(
        "Cloud Run baixa o mês do Kaggle, grava raw (parquet) e bronze "
        "(Iceberg) em GCS; em seguida BQ aponta uma tabela externa BigLake "
        "para o metadata.json mais recente."
    ),
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 12, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    default_args=build_default_args(owner="alef_rp"),
    tags=["ingestion", "cloud-run", "iceberg", "biglake"],
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
    def discover_latest_metadata() -> str:
        return latest_metadata_uri(load_gcp_config())

    @task
    def build_ddl(metadata_uri: str) -> str:
        return build_external_iceberg_ddl(load_gcp_config(), metadata_uri)

    metadata_uri = discover_latest_metadata()
    ddl = build_ddl(metadata_uri)

    refresh_external = BigQueryInsertJobOperator(
        task_id="refresh_bronze_external_table",
        configuration={"query": {"query": ddl, "useLegacySql": False}},
        gcp_conn_id=_gcp.bq_conn_id,
        location=_gcp.bq_location,
    )

    ingest >> metadata_uri >> ddl >> refresh_external


gcs_to_bigquery_dag()
