from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from script.extract import extract
from script.check_data import check_data
from script.clean_data import clean_data
from script.load_data import load_data


default_args = {
    'owner': 'ap1911ak',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG (
    'mini_sales_pipeline',
    default_args=default_args,
    description='A simple mini sales pipeline',
    schedule_interval='@daily',
    catchup=False,
)as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        dag=dag
    )

    clean_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        dag=dag
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag
    )

    extract_task >> clean_task >> load_task