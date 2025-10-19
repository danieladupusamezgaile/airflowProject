from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime

DATASET_COCKTAIL = Dataset("/tmp/random_cocktail.json")

def _get_cocktail():
    import requests

    try:
        r = requests.get("https://www.thecocktaildb.com/api/json/v1/1/random.php")
        r.raise_for_status()
        with open(DATASET_COCKTAIL.uri, 'wb') as f:
            f.write(r.content)
    except Exception as e:
        print(f"API currently not available. Error: {e}")

@dag(
    start_date=datetime(2025, 10, 16),
    schedule="@daily",
    catchup=False
)
def extractor():
    get_cocktail = PythonOperator(
        task_id='get_cocktail',
        python_callable=_get_cocktail,
        outlets=[DATASET_COCKTAIL]
    )
extractor()
