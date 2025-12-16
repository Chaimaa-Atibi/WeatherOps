from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'othmane',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_ops_pipeline',
    default_args=default_args,
    description='Pipeline météo complet',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 1. Ingestion
    task_ingestion = BashOperator(
        task_id='fetch_weather_data',
        bash_command='python3 /home/laaouinateothman/code/othmane12366677/WeatherOps/ingestion/fetch_weather.py'
    )

    # 2. Transformation
    task_transform = BashOperator(
        task_id='transform_weather_data',
        bash_command='python3 /home/laaouinateothman/code/othmane12366677/WeatherOps/transformation/transform_data.py'
    )

    # 3. Chargement BigQuery
    task_load = BashOperator(
        task_id='load_to_bigquery',
        bash_command='python3 /home/laaouinateothman/code/othmane12366677/WeatherOps/data_warehouse/load_to_bigquery.py'
    )

    # Ordre des tâches
    task_ingestion >> task_transform >> task_load