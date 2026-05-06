from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
import random
from datetime import datetime


with DAG(
    "teste_pools",
    description="DAG de exemplo",
    start_date=datetime(2022, 1, 1), 
    schedule_interval="@hourly", 
    catchup=False,
    tags=["test"],
    ) as dag:

    def gerar_numero():
        return random.randint(1, 10)
    
    def selecionar_tarefa(**kwargs):
        numero = kwargs['ti'].xcom_pull(task_ids='gerar_numero')
        if numero > 5:
            return 'task5'
        else:
            return 'task6'

    task1 = BashOperator(task_id="task1", bash_command="sleep 2")
    task2 = BashOperator(task_id="task2", bash_command="sleep 5", pool="pool_2", priority_weight=2)
    task3 = BashOperator(task_id="task3", bash_command="sleep 5", pool="pool_2", priority_weight=3)
    task4 = BashOperator(task_id="task4", bash_command="sleep 5", pool="pool_2", priority_weight=1)
    
    gerar_numero = PythonOperator(
        task_id="gerar_numero",
        python_callable=gerar_numero
    )

    selecionar_tarefa = BranchPythonOperator(
        task_id="selecionar_tarefa",
        python_callable=selecionar_tarefa
    )

    task5 = BashOperator(task_id="task5", bash_command="sleep 5")
    task6 = BashOperator(task_id="task6", bash_command="sleep 2")

    [task1, task2, task3, task4] >> gerar_numero >> selecionar_tarefa >> [task5, task6]