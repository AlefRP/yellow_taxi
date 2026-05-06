"""Testes do builder de configuração de load BigQuery."""

from __future__ import annotations

from datetime import datetime

import pytest

from include.bq_loader import build_parquet_load_config
from include.config import GcpConfig


@pytest.fixture
def cfg() -> GcpConfig:
    return GcpConfig(
        project_id="proj",
        region="us-central1",
        bq_dataset="ds",
        bq_raw_table="raw_yt",
        bq_location="US",
        gcs_bucket="my-bucket",
        gcs_raw_prefix="raw/yellow_tripdata",
        gcs_bronze_prefix="bronze/yellow_taxi",
    )


def test_uri_uses_year_month_zero_padded(cfg):
    out = build_parquet_load_config(datetime(2024, 3, 1), cfg)
    [uri] = out["load"]["sourceUris"]
    assert uri == "gs://my-bucket/raw/yellow_tripdata/yellow_tripdata_2024-03.parquet"


def test_uri_pads_single_digit_month(cfg):
    out = build_parquet_load_config(datetime(2024, 1, 1), cfg)
    [uri] = out["load"]["sourceUris"]
    assert "2024-01" in uri


def test_destination_uses_config(cfg):
    out = build_parquet_load_config(datetime(2024, 5, 1), cfg)
    dest = out["load"]["destinationTable"]
    assert dest == {
        "projectId": "proj",
        "datasetId": "ds",
        "tableId": "raw_yt",
    }


def test_load_options_are_correct(cfg):
    out = build_parquet_load_config(datetime(2024, 5, 1), cfg)
    load = out["load"]
    assert load["sourceFormat"] == "PARQUET"
    assert load["writeDisposition"] == "WRITE_APPEND"
    assert load["autodetect"] is True
