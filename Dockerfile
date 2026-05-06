FROM quay.io/astronomer/astro-runtime:12.7.0

# Virtualenv isolado para o dbt — necessário pelo Cosmos com ExecutionConfig.
RUN python -m venv /usr/local/airflow/dbt_venv \
    && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.9.1 \
        dbt-bigquery==1.9.1
