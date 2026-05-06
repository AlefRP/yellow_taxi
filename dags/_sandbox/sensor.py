from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests

def process_api():
    
    response = requests.get("https://httpbin.org/get")
    data = response.json()
    df = pd.DataFrame(data)
    print(df)

with DAG(
    "sensor",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="httpbin_api",
        endpoint="get",              
        poke_interval=5,
        timeout=20
    )

   
    process_api_task = PythonOperator(
        task_id="process_api",
        python_callable=process_api
    )
    
    is_api_available >> process_api_task
