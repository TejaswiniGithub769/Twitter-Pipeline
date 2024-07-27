from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Importing your ETL function from your script
from Twitter_etl import run_twitter_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 6, 27),  
    'email': ['tejaswini@gmail.com'],  
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitter_etl_dag',  # Updated DAG name
    default_args=default_args,
    description='A DAG for running Twitter ETL process',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='run_twitter_etl_task',
    python_callable=run_twitter_etl,
    dag=dag, 
)

run_etl
