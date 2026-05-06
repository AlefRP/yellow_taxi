"""
Helpers para a tabela externa Iceberg no BigQuery (BigLake).

Mantemos a lógica pura aqui para ser unit-testável sem precisar do GCS/BQ.
"""

from __future__ import annotations

import re

from google.cloud import storage

from include.config import GcpConfig

# Padrão de nome de metadata.json escrito pelo pyiceberg:
#   00000-{uuid}.metadata.json, 00001-{uuid}.metadata.json, ...
_METADATA_RE = re.compile(r"(?P<seq>\d+)-[0-9a-f-]+\.metadata\.json$")


def latest_metadata_uri(cfg: GcpConfig, *, namespace: str = "yellow_taxi",
                        table: str = "bronze") -> str:
    """Encontra o `vN.metadata.json` mais recente da tabela bronze em GCS.

    Conforme o pyiceberg, os arquivos vivem em
    `gs://{bucket}/{bronze_prefix}/{namespace}.db/{table}/metadata/`.
    """
    prefix = f"{cfg.gcs_bronze_prefix}/{namespace}.db/{table}/metadata/"

    client = storage.Client()
    blobs = client.list_blobs(cfg.gcs_bucket, prefix=prefix)
    candidates = [
        (int(m.group("seq")), b.name)
        for b in blobs
        if (m := _METADATA_RE.search(b.name))
    ]
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum metadata.json encontrado em gs://{cfg.gcs_bucket}/{prefix}"
        )
    candidates.sort()
    return f"gs://{cfg.gcs_bucket}/{candidates[-1][1]}"


def build_external_iceberg_ddl(
    cfg: GcpConfig, metadata_uri: str, *, table: str = "bronze_yellow_taxi"
) -> str:
    """Monta `CREATE OR REPLACE EXTERNAL TABLE` apontando para o metadata.json
    Iceberg em GCS.
    """
    return (
        f"CREATE OR REPLACE EXTERNAL TABLE "
        f"`{cfg.project_id}.{cfg.bq_dataset}.{table}`\n"
        f"WITH CONNECTION `{cfg.biglake_connection}`\n"
        f"OPTIONS (\n"
        f"  format = 'ICEBERG',\n"
        f"  uris = ['{metadata_uri}']\n"
        f")"
    )
