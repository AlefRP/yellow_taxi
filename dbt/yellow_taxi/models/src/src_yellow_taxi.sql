-- Espelha a tabela externa Iceberg `bronze_yellow_taxi` (BigLake), criada
-- pelo DAG `gcs_to_bigquery` apontando para os arquivos Iceberg em GCS.
SELECT *
FROM {{ source('bronze', 'bronze_yellow_taxi') }}
