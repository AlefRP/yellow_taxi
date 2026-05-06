# Yellow Taxi — Pipeline (Cloud Run + GCS + BigQuery + dbt)

Pipeline de engenharia de dados para as viagens de táxi amarelo de NYC.
Ingestão via **Cloud Run Job** (Kaggle → GCS), carga em **BigQuery** a partir
do parquet raw, e modelagem com **dbt** orquestrada pelo **Apache Airflow**
(Astronomer Runtime, via Astronomer Cosmos).

A camada **bronze em Iceberg** é gravada em GCS pela Cloud Run para consumo
por outros mecanismos (Spark/Trino) — o BigQuery deste pipeline lê apenas a
camada raw via `LOAD` job.

## Arquitetura

```text
   Kaggle
     │
     ▼
  ┌─────────────────────────┐
  │  Cloud Run Job          │  (disparado pelo Airflow,
  │  yellow-taxi-ingest     │   CloudRunExecuteJobOperator)
  └────────────┬────────────┘
               │
               │ escreve em GCS
               ▼
  ┌─────────────────────────────────────────────────────────────┐
  │                          GCS                                │
  │   raw/yellow_tripdata/yellow_tripdata_YYYY-MM.parquet       │
  │   bronze/yellow_taxi/yellow_taxi.db/bronze/{data,metadata}/ │
  └────────────┬────────────────────────────────────────────────┘
               │ (somente raw)
               ▼
   ┌──────────────────────┐    yellow_taxi (diário, Cosmos)
   │   BQ raw_yellow_taxi │ ─────────────────────────────────▶  dbt
   │   (LOAD parquet)     │                                     src → staging → marts
   └──────────────────────┘
```

### Camadas

| Camada | Local | Formato | Produzido por | Consumido por |
| --- | --- | --- | --- | --- |
| **raw** | `gs://{bucket}/raw/yellow_tripdata/` | Parquet (1 arquivo/mês) | Cloud Run Job | BigQuery LOAD job |
| **bronze** | `gs://{bucket}/bronze/yellow_taxi/` | Iceberg | Cloud Run Job (pyiceberg) | Outros engines (Spark/Trino) |
| **raw (BQ)** | `{dataset}.{raw_table}` | BQ table | Airflow (LOAD) | dbt |
| **staging/marts** | `{dataset}.stg_*`, `{dataset}.fct_*` | BQ tables | dbt | Consumo analítico |

## Estrutura

```text
.
├── dags/                              # DAGs de produção
│   ├── gcs_to_bigquery.py             # Cloud Run + BQ external table refresh
│   ├── yellow_taxi.py                 # dbt build via Cosmos
│   └── _sandbox/                      # DAGs de estudo (ignoradas pelo scheduler)
├── dbt/yellow_taxi/                   # Projeto dbt
│   └── models/{src,staging,marts}/
├── include/
│   ├── cloud_run/yellow_taxi_ingest/  # Código do Cloud Run Job
│   ├── config.py                      # Config centralizada (Variable→env→default)
│   ├── default_args.py                # Default args padrão dos DAGs
│   ├── callbacks.py                   # on_failure
│   ├── bq_loader.py                   # Builder do load job (parquet → BQ)
│   ├── profiles.py                    # ProfileConfig do Cosmos
│   └── constants.py
├── tests/                             # pytest
└── .github/workflows/ci.yml           # CI
```

## Pré-requisitos

- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) ≥ 1.28
- Docker Desktop
- Projeto GCP com **BigQuery**, **GCS** e **Cloud Run** habilitados
- Service account do Airflow com:
  - `roles/run.invoker`, `roles/run.developer` (Cloud Run)
  - `roles/bigquery.dataEditor`, `roles/bigquery.jobUser`
  - `roles/storage.objectViewer`
- Service account do Cloud Run Job com:
  - `roles/storage.objectAdmin` no bucket (escrever raw/ e bronze/)
- Credenciais Kaggle (`KAGGLE_USERNAME`, `KAGGLE_KEY`) — locais em `.env`,
  passadas ao Cloud Run Job via `--set-env-vars` no deploy

## Setup local

```powershell
Copy-Item .env.example .env
# editar .env com os valores do seu projeto

# subir o Airflow local
astro dev start
```

UI em <http://localhost:8080> (admin/admin).

## Cloud Run Job — build & deploy

Ver [include/cloud_run/yellow_taxi_ingest/README.md](include/cloud_run/yellow_taxi_ingest/README.md).

## Variáveis e Connections

Todas as variáveis estão documentadas em `.env.example`. Em produção, espelhe-as
como **Airflow Variables** (precedência > env). Connections obrigatórias:

- `my_bigquery_connection` — Google Cloud (mesma conn é reutilizada pelo
  `CloudRunExecuteJobOperator` e pelo `BigQueryInsertJobOperator`)

## Comandos úteis

```powershell
astro dev start
astro dev pytest tests/

# dbt dentro do container
astro dev bash
source /usr/local/airflow/dbt_venv/bin/activate
cd /usr/local/airflow/dbt/yellow_taxi
dbt deps && dbt build
```

## Testes

```powershell
astro dev pytest tests/
```

Cobertura:

- `tests/dags/test_dag_integrity.py` — DagBag, tags, owner, retries, ciclos
- `tests/test_config.py` — precedência env/default; `GcpConfig` e `CloudRunConfig`
- `tests/test_default_args.py` — `build_default_args` correto
- `tests/test_callbacks.py` — `on_failure` não levanta
- `tests/test_bq_loader.py` — URI/destino/options do load BigQuery

## CI

[GitHub Actions](.github/workflows/ci.yml) em cada push/PR para `main`:

- **lint**: `ruff check` + `ruff format --check`
- **test**: instala Airflow + deps, roda `pytest`
- **dbt**: `dbt deps`, `dbt parse` (profile stub em `dbt/yellow_taxi/ci/`)
  e `sqlfluff lint` (informativo)

## Convenções

- **DAGs**: `snake_case`, com `description`, `tags`, e `default_args = build_default_args()`
- **Modelos dbt**: `src_<source>`, `stg_<entidade>`, `fct_<entidade>`/`dim_<entidade>`
- **Testes dbt**: `not_null` + `unique` em chaves; `relationships` em FKs

## Deploy (Astronomer)

```powershell
astro deploy
```

Ver <https://www.astronomer.io/docs/astro/deploy-code>.
