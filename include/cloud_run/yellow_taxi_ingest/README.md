# Cloud Run Job — yellow_taxi_ingest

Job batch que baixa um mês do dataset NYC Yellow Taxi do Kaggle e grava como
Parquet em `gs://{BUCKET}/raw/yellow_tripdata/yellow_tripdata_{YYYY}-{MM}.parquet`.

A camada **bronze (Iceberg)** é construída a jusante pelo dbt — não por este job.

## Variáveis

Setadas no Job (uma vez):

| Nome | Default |
| --- | --- |
| `BUCKET_NAME` | — (obrigatório) |
| `GCS_RAW_PREFIX` | `raw/yellow_tripdata` |
| `KAGGLE_DATASET` | `elemento/nyc-yellow-taxi-trip-data` |
| `KAGGLE_USERNAME` | via Secret Manager |
| `KAGGLE_KEY` | via Secret Manager |

Injetadas por execução pelo `CloudRunExecuteJobOperator`:

| Nome | Origem |
| --- | --- |
| `YEAR` | `data_interval_start.year` |
| `MONTH` | `data_interval_start.month` |

## Build & deploy

```bash
PROJECT=meu-projeto
REGION=us-central1
REPO=cloud-run-jobs
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/yellow-taxi-ingest:latest"

# Build
gcloud builds submit --tag "$IMAGE" .

# Cria o Job (uma vez)
gcloud run jobs create yellow-taxi-ingest \
  --image "$IMAGE" \
  --region "$REGION" \
  --task-timeout 30m \
  --memory 2Gi \
  --cpu 2 \
  --max-retries 1 \
  --set-env-vars BUCKET_NAME=meu-bucket,GCS_RAW_PREFIX=raw/yellow_tripdata \
  --set-secrets KAGGLE_USERNAME=kaggle-username:latest,KAGGLE_KEY=kaggle-key:latest

# Atualizações posteriores
gcloud run jobs update yellow-taxi-ingest --image "$IMAGE" --region "$REGION"
```
