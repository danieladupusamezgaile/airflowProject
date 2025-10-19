from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime

DATASET_COCKTAIL = Dataset("/tmp/random_cocktail.json")

def _get_cocktail(**context): # ti - task instance
    ti = context["ti"]
    import requests

    try:
        r = requests.get("https://www.thecocktaildb.com/api/json/v1/1/random.php")
        r.raise_for_status()
        with open(DATASET_COCKTAIL.uri, 'wb') as f:
            f.write(r.content)
        ti.xcom_push(key="request_size", value=len(r.content))
    except Exception as e:
        print(f"API currently not available. Error: {e}")

def _check_size(**context):
    ti = context["ti"]
    request_size = ti.xcom_pull(task_ids='get_cocktail', key='request_size')
    print(f"Size of the request: {request_size}")

@dag(
    start_date=datetime(2025, 10, 10),
    schedule="@daily",
    catchup=False
)
def extractor():
    get_cocktail = PythonOperator(
        task_id='get_cocktail',
        python_callable=_get_cocktail,
        outlets=[DATASET_COCKTAIL]
    )
    
    check_size = PythonOperator(
        task_id='check_size',
        python_callable=_check_size
    )

    get_cocktail >> check_size
    
extractor()
