"""Testes do builder de DDL e do parser de metadata.json."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from include.bq_iceberg import (
    _METADATA_RE,
    build_external_iceberg_ddl,
    latest_metadata_uri,
)
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
        biglake_connection="proj.us.biglake_conn",
    )


# --------------------------------------------------------------------------
# build_external_iceberg_ddl
# --------------------------------------------------------------------------

def test_ddl_uses_project_dataset_table(cfg):
    ddl = build_external_iceberg_ddl(cfg, "gs://x/m.json")
    assert "`proj.ds.bronze_yellow_taxi`" in ddl
    assert "WITH CONNECTION `proj.us.biglake_conn`" in ddl


def test_ddl_sets_iceberg_format_and_metadata_uri(cfg):
    ddl = build_external_iceberg_ddl(cfg, "gs://my-bucket/bronze/.../v3.metadata.json")
    assert "format = 'ICEBERG'" in ddl
    assert "uris = ['gs://my-bucket/bronze/.../v3.metadata.json']" in ddl


def test_ddl_table_name_override(cfg):
    ddl = build_external_iceberg_ddl(cfg, "gs://x/m.json", table="other_t")
    assert "`proj.ds.other_t`" in ddl


# --------------------------------------------------------------------------
# Regex de metadata
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "name,expected_seq",
    [
        ("00000-abcd1234.metadata.json", 0),
        ("00001-deadbeef-1234-5678-9abc-def012345678.metadata.json", 1),
        ("00123-xxxxxxxx.metadata.json", 123),
    ],
)
def test_metadata_regex_matches_pyiceberg_filenames(name, expected_seq):
    m = _METADATA_RE.search(name)
    assert m is not None
    assert int(m.group("seq")) == expected_seq


@pytest.mark.parametrize(
    "name",
    [
        "snap-1234.avro",
        "version-hint.text",
        "0000.metadata.json",          # sem hífen+uuid
        "00000-.metadata.json",        # uuid vazio
    ],
)
def test_metadata_regex_rejects_others(name):
    assert _METADATA_RE.search(name) is None


# --------------------------------------------------------------------------
# latest_metadata_uri
# --------------------------------------------------------------------------

def _make_blob(name: str):
    b = MagicMock()
    b.name = name
    return b


def test_latest_metadata_uri_picks_highest_sequence(cfg):
    fake_blobs = [
        _make_blob("bronze/yellow_taxi/yellow_taxi.db/bronze/metadata/00000-aaa.metadata.json"),
        _make_blob("bronze/yellow_taxi/yellow_taxi.db/bronze/metadata/00002-ccc.metadata.json"),
        _make_blob("bronze/yellow_taxi/yellow_taxi.db/bronze/metadata/00001-bbb.metadata.json"),
        _make_blob("bronze/yellow_taxi/yellow_taxi.db/bronze/metadata/snap-x.avro"),
    ]
    with patch("include.bq_iceberg.storage.Client") as client_cls:
        client_cls.return_value.list_blobs.return_value = fake_blobs
        uri = latest_metadata_uri(cfg)

    assert uri.endswith("/00002-ccc.metadata.json")
    assert uri.startswith("gs://my-bucket/")


def test_latest_metadata_uri_raises_when_empty(cfg):
    with patch("include.bq_iceberg.storage.Client") as client_cls:
        client_cls.return_value.list_blobs.return_value = []
        with pytest.raises(FileNotFoundError):
            latest_metadata_uri(cfg)
