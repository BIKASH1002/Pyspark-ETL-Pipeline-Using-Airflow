from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import warnings
warnings.filterwarnings("ignore")
from includes.etl import run_etl  

default_args = {
    'owner': 'bikash',
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes = 5),
}

with DAG(
    default_args = default_args,
    dag_id = 'Netflix_ETL_Pipeline',
    description = 'ETL Pipeline using PySpark',
    schedule_interval = '@daily'
) as dag:

    run_etl_task = PythonOperator(
        task_id = 'run_etl',
        python_callable = run_etl,  
    )

    run_etl_task
