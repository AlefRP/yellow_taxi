from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    "dag_xcom",
    description="DAG de exemplo",
    start_date=datetime(2022, 1, 1), 
    schedule_interval="@hourly", 
    catchup=False,
    tags=["test"],
    ) as dag:

    def task_write(**kwargs):
        kwargs['ti'].xcom_push(key='value', value='Mensagem')

    def task_read(**kwargs):
        value = kwargs['ti'].xcom_pull(key='value')
        print(f'Valor recebido: {value}')

    task1 = BashOperator(task_id="task1", bash_command="sleep 5", retries=3, do_xcom_push=False)
    task2 = PythonOperator(
        task_id="task2", 
        python_callable=task_write
        )
    
    task3 = PythonOperator(
        task_id="task3", 
        python_callable=task_read
        )

    task1 >> task2 >> task3