from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'email': ['R2T5w@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'do_xcom_push': False
}

def printar_variavel():
    VALOR_VARIAVEL = Variable.get("variavel_teste")
    print(f"Valor da variável: {VALOR_VARIAVEL}")


with DAG(
    "dag_teste",
    description="DAG de teste",
    start_date=datetime(2022, 1, 1), 
    schedule_interval="@hourly", 
    catchup=False,
    tags=["test"],
    default_args=default_args,
    default_view="graph"
    ) as dag:

    task1 = BashOperator(task_id="task1", bash_command="sleep 5", retries=3)
    task2 = BashOperator(task_id="task2", bash_command="sleep 5")
    task3 = BashOperator(task_id="task3", bash_command="sleep 5")

    with TaskGroup("group1") as group1:
        task4 = BashOperator(task_id="task4", bash_command="sleep 5")
        task5 = BashOperator(task_id="task5", bash_command="sleep 5")
    
    emp = EmptyOperator(task_id="empty")

    task6 = BashOperator(task_id="task6", bash_command="sleep 5")
    task7 = TriggerDagRunOperator(
        task_id="task7",
        trigger_dag_id="dag_xcom",
        conf={"key": "value"}
    )

    printa_valor = PythonOperator(
        task_id="printa_valor",
        python_callable=printar_variavel
    )

    [task1, task2, task3] >> emp >> [group1, task6, task7] >> printa_valor