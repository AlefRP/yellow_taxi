from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


meu_dataset = Dataset('/usr/local/airflow/data/churn_novo.csv')

with DAG(
    "consumer",
    description="Extrai, transforma e carrega os dados de churn",
    start_date=datetime(2025, 1, 1),
    schedule=[meu_dataset],
    catchup=False,
) as dag:
    
    def meu_dataset():

        dataset = pd.read_csv('/usr/local/airflow/data/churn_novo.csv', sep=';')
        dataset.to_csv('/usr/local/airflow/data/churn_novo_atualizado.csv', sep=';', index=False)

    inicio = EmptyOperator(task_id="inicio")

    consumidor = PythonOperator(
        task_id="consumidor",
        python_callable=meu_dataset,
        provide_context=True
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> consumidor >> fim