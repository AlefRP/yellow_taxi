"""
Cloud Run Job — ingere o dataset NYC Yellow Taxi do Kaggle em duas camadas:

    raw     →  gs://{BUCKET}/{RAW_PREFIX}/yellow_tripdata_{YYYY}-{MM}.parquet
    bronze  →  gs://{BUCKET}/{BRONZE_PREFIX}/yellow_taxi/  (formato Iceberg)

A camada **raw** preserva os arquivos originais para replay/backfill. A
**bronze** acumula os meses como tabela Iceberg, lida pelo BigQuery via
external table (BigLake) — vide DAG `gcs_to_bigquery`.

Para evitar a complexidade de um catálogo Iceberg externo (REST/Glue/etc.),
usamos um **SqlCatalog SQLite persistido como blob no próprio bucket**:
o job baixa, escreve e re-uploads o arquivo `_catalog/catalog.db`. O
agendamento mensal com `max_active_runs=1` garante writer único.

Variáveis de ambiente:

    YEAR              ano (injetado pelo Airflow por execução)
    MONTH             mês 1-12 (injetado pelo Airflow por execução)
    BUCKET_NAME       bucket GCS de destino (obrigatório)
    GCS_RAW_PREFIX    default "raw/yellow_tripdata"
    GCS_BRONZE_PREFIX default "bronze/yellow_taxi"
    KAGGLE_DATASET    default "elemento/nyc-yellow-taxi-trip-data"
    KAGGLE_USERNAME   credencial Kaggle (Secret Manager)
    KAGGLE_KEY        credencial Kaggle (Secret Manager)
"""

from __future__ import annotations

import io
import logging
import os
import sys
import tempfile
from pathlib import Path

import kagglehub
import pandas as pd
import pyarrow as pa
from google.cloud import storage
from kagglehub import KaggleDatasetAdapter
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("yellow_taxi_ingest")

# Identidade lógica da tabela Iceberg dentro do catálogo.
ICEBERG_NAMESPACE = "yellow_taxi"
ICEBERG_TABLE = "bronze"
CATALOG_BLOB = "_catalog/catalog.db"


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"variável de ambiente obrigatória ausente: {name}")
    return value


def _required_int_env(name: str) -> int:
    raw = _required_env(name)
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} deve ser inteiro, recebido: {raw!r}") from exc


def _kaggle_filename(year: int, month: int) -> str:
    return f"yellow_tripdata_{year}-{month:02d}.csv"


def _raw_blob_name(prefix: str, year: int, month: int) -> str:
    return f"{prefix}/yellow_tripdata_{year}-{month:02d}.parquet"


# --------------------------------------------------------------------------
# Transformações de schema (raw → bronze)
# --------------------------------------------------------------------------

BRONZE_RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "store_and_fwd_flag": "store_and_fwd_flag",
}
BRONZE_COLUMNS = list(BRONZE_RENAMES.values()) + ["pickup_date"]


def to_bronze(df: pd.DataFrame) -> pa.Table:
    """Renomeia, tipa e seleciona colunas finais da camada bronze."""
    df = df.rename(columns=BRONZE_RENAMES).copy()
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    df["pickup_date"] = df["pickup_datetime"].dt.date
    df["vendor_id"] = pd.to_numeric(df["vendor_id"], errors="coerce").astype("Int64")
    df["passenger_count"] = pd.to_numeric(
        df["passenger_count"], errors="coerce"
    ).astype("Int64")
    return pa.Table.from_pandas(df[BRONZE_COLUMNS], preserve_index=False)


# --------------------------------------------------------------------------
# GCS I/O
# --------------------------------------------------------------------------

def upload_parquet(df: pd.DataFrame, bucket: str, blob_name: str) -> str:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client = storage.Client()
    blob = client.bucket(bucket).blob(blob_name)
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    uri = f"gs://{bucket}/{blob_name}"
    log.info("Upload raw OK: %s", uri)
    return uri


def download_catalog_db(bucket: str, blob_name: str, dest: Path) -> bool:
    """Baixa o sqlite do catálogo se existir. Retorna True se baixou."""
    client = storage.Client()
    blob = client.bucket(bucket).blob(blob_name)
    if not blob.exists():
        return False
    blob.download_to_filename(dest)
    log.info("Catálogo baixado de gs://%s/%s", bucket, blob_name)
    return True


def upload_catalog_db(bucket: str, blob_name: str, src: Path) -> None:
    client = storage.Client()
    blob = client.bucket(bucket).blob(blob_name)
    blob.upload_from_filename(src)
    log.info("Catálogo persistido em gs://%s/%s", bucket, blob_name)


# --------------------------------------------------------------------------
# Iceberg
# --------------------------------------------------------------------------

def append_to_bronze(
    arrow_table: pa.Table, bucket: str, bronze_prefix: str
) -> str:
    """Append na tabela Iceberg bronze. Retorna o caminho do metadata.json novo."""
    with tempfile.TemporaryDirectory() as tmp:
        local_db = Path(tmp) / "catalog.db"
        download_catalog_db(bucket, CATALOG_BLOB, local_db)

        warehouse = f"gs://{bucket}/{bronze_prefix}"
        catalog = SqlCatalog(
            "default",
            **{
                "uri": f"sqlite:///{local_db}",
                "warehouse": warehouse,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )
        catalog.create_namespace_if_not_exists(ICEBERG_NAMESPACE)

        identifier = (ICEBERG_NAMESPACE, ICEBERG_TABLE)
        try:
            table = catalog.load_table(identifier)
            log.info("Tabela Iceberg existente carregada")
        except NoSuchTableError:
            log.info("Tabela Iceberg não existe — criando")
            table = catalog.create_table(
                identifier,
                schema=arrow_table.schema,
            )

        table.append(arrow_table)
        log.info("Append OK: %s linhas adicionadas", arrow_table.num_rows)

        upload_catalog_db(bucket, CATALOG_BLOB, local_db)

        return table.metadata_location


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main() -> int:
    year = _required_int_env("YEAR")
    month = _required_int_env("MONTH")
    if not 1 <= month <= 12:
        raise RuntimeError(f"MONTH fora de [1, 12]: {month}")

    bucket = _required_env("BUCKET_NAME")
    raw_prefix = os.environ.get("GCS_RAW_PREFIX", "raw/yellow_tripdata")
    bronze_prefix = os.environ.get("GCS_BRONZE_PREFIX", "bronze/yellow_taxi")
    dataset = os.environ.get("KAGGLE_DATASET", "elemento/nyc-yellow-taxi-trip-data")

    file_path = _kaggle_filename(year, month)
    log.info("Baixando %s/%s", dataset, file_path)
    df = kagglehub.load_dataset(KaggleDatasetAdapter.PANDAS, dataset, file_path)
    log.info("Linhas: %s", len(df))

    upload_parquet(df, bucket, _raw_blob_name(raw_prefix, year, month))

    arrow_bronze = to_bronze(df)
    metadata_uri = append_to_bronze(arrow_bronze, bucket, bronze_prefix)
    log.info("Bronze metadata: %s", metadata_uri)
    return 0


if __name__ == "__main__":
    sys.exit(main())
