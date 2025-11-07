from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.ingestion.bcb_ptax import run_bcb_ingestion
from src.ingestion.ibge_ipca import run_ipca_ingestion
from src.ingestion.eia_petroleo import run_eia_ingestion

default_args = {
    "owner": "openmacrolake",
    "depends_on_past": False,
    "email": ["admin@openmacrolake.io"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="pipeline_macro_diario",
    description="ETL: BCB, IBGE e EIA (dados econômicos 2024+)",
    schedule_interval="0 8 * * *", 
    start_date=datetime(2025, 11, 1),
    catchup=False,
    default_args=default_args,
    tags=["macro", "economia", "open-source"]
) as dag:

    t_bcb = PythonOperator(
        task_id="ingestao_bcb_ptax",
        python_callable=run_bcb_ingestion,
    )

    t_ipca = PythonOperator(
        task_id="ingestao_ibge_ipca",
        python_callable=run_ipca_ingestion,
    )

    t_eia = PythonOperator(
        task_id="ingestao_eia_petroleo",
        python_callable=run_eia_ingestion,
    )

    [t_bcb, t_ipca, t_eia]
