from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


with DAG(
    "producer",
    description="Extrai, transforma e carrega os dados de churn",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    def carrega_arquivo():

        dataset = pd.read_csv('/usr/local/airflow/data/churn.csv', sep=';')
        dataset.to_csv('/usr/local/airflow/data/churn_novo.csv', sep=';', index=False)

    inicio = EmptyOperator(task_id="inicio")

    meu_dataset = Dataset('/usr/local/airflow/data/churn_novo.csv')

    produtor = PythonOperator(
        task_id="produtor",
        python_callable=carrega_arquivo,
        outlets=[meu_dataset]
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> produtor >> fim