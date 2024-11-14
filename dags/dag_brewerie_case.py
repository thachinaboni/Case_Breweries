from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from Case_Breweries.dags.tasks import breweries_etl

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
    begin_data = DummyOperator(
        task_id="begin_data"
    )

    end_data = DummyOperator(
        task_id="end_data"
    )

    breweries_case = PythonOperator(
        task_id="breweries_case",
        python_callable=breweries_case.breweries_case
    )

    # Sequência de execução:
    begin_data >> breweries_case >> end_data
