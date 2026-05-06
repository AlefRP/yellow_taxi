"""
Builders puros para configurações de jobs BigQuery.

Separados dos DAGs para serem unit-testáveis sem Airflow.
"""

from __future__ import annotations

from datetime import datetime

from include.config import GcpConfig


def build_parquet_load_config(data_interval_start: datetime, cfg: GcpConfig) -> dict:
    """Monta o `configuration` para `BigQueryInsertJobOperator` carregar
    o parquet mensal de yellow_tripdata correspondente ao mês de
    `data_interval_start`.
    """
    year = data_interval_start.year
    month = data_interval_start.month

    gcs_uri = (
        f"gs://{cfg.gcs_bucket}/{cfg.gcs_raw_prefix}/" f"yellow_tripdata_{year}-{month:02d}.parquet"
    )

    return {
        "load": {
            "sourceUris": [gcs_uri],
            "destinationTable": {
                "projectId": cfg.project_id,
                "datasetId": cfg.bq_dataset,
                "tableId": cfg.bq_raw_table,
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_APPEND",
            "autodetect": True,
        }
    }
