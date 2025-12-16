from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

from scraper_functions import scrape_and_store_data

logger = logging.getLogger(__name__)

DB_CONFIG = {
    'username': 'postgres',
    'password': '7510',
    'host': '172.30.176.1',
    'port': '5432',
    'database': 'house_prices'
}

SCRAPING_CONFIG = {
    'start_page': 1,
    'end_page': 4
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'house_scraper_daily',
    default_args=default_args,
    description='Daily scraping of house prices from buyrentkenya.com',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['scraping', 'real-estate', 'etl'],
    max_active_runs=1,
) as dag:
    
    scrape_task = PythonOperator(
        task_id='scrape_houses',
        python_callable=scrape_and_store_data,
        op_kwargs={'db_config': DB_CONFIG, 'scraping_config': SCRAPING_CONFIG},
        provide_context=True,
    )
