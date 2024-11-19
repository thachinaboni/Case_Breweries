import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from scripts import breweries_etl

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Definir as configurações do DAG
default_args = {
    'owner': 'Thainá Bonifacio',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inicializar o DAG
with DAG(
    'breweries_data_pipeline',
    default_args=default_args,
    description='ETL para dados de cervejarias',
    schedule_interval=timedelta(days=1),  # Executa diariamente
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    dag.doc_md = """
    This DAG perform all ETL to obtain data from the Breweries API.
    It starts by getting the data from the API and saves it in the RAW layer. 
    1. Bronze Layer: Saving raw data in JSON format.
    2. Silver Layer: Data transformation, filling null values ​and rounding the latitude and longitude columns. The data is saved in Parquet format, partitioned by state.
    3. Gold Layer: Aggregation of data, calculating the number of breweries by type, state and city. Aggregated data is saved in Parquet format.
    If you have any questions, please feel free to contact the data engineer responsible for DAG:
    - Github: @thachinaboni
    """
    begin_data = DummyOperator(
        task_id="begin_data"
    )

    end_data = DummyOperator(
        task_id="end_data"
    )

    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=breweries_etl.fetch_data
    )

    save_bronze_task = PythonOperator(
        task_id="save_bronze",
        python_callable=breweries_etl.save_bronze,
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=breweries_etl.transform_data,
    )

    save_silver_task = PythonOperator(
        task_id="save_silver",
        python_callable=breweries_etl.save_silver,
    )

    create_gold_layer_task = PythonOperator(
        task_id='create_gold_layer',
        python_callable=breweries_etl.create_gold_layer,
    )

    # Sequência de execução:
    begin_data >> fetch_data_task >> save_bronze_task >> transform_data_task >>  save_silver_task >> create_gold_layer_task >> end_data