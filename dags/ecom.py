from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags.extractor import DATASET_COCKTAIL

@dag(start_date=datetime(2025, 10, 15),
     schedule=[DATASET_COCKTAIL], # schedule based on dataset availability
     catchup=True, # do not backfill. Only run from start_date onward
     description='ETL DAG for e-commerce data',
     tags=['ecom'],
     default_args={'owner': 'airflow', 'retries': 1},  # Define default arguments for all tasks in the DAG
     dagrun_timeout=timedelta(minutes=60), # maximum time allowed for a DAG to run
     max_consecutive_failed_dag_runs=3, # if u have 3 consecutive failed runs, pause the DAG
     max_active_runs=1  # only allow one active run at a time
)
def ecom():
    def extract():
        print("Extracting data...")

    def transform():
        print("Transforming data...")

    def load():
        print("Loading data...")

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load
    )

    extract_task >> transform_task >> load_task
ecom()
