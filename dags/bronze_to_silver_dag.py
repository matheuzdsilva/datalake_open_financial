from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "openmacrolake",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="bronze_to_silver_dag",
    default_args=default_args,
    description="Pipeline OpenMacroLake - Bronze to Silver",
    schedule_interval=None,  
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["openmacrolake", "spark", "etl"],
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id="spark_bronze_to_silver",
        application="/opt/airflow/src/processing/bronze_to_silver.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[],
        conf={
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
            "spark.hadoop.security.authentication": "simple",
            "spark.hadoop.hadoop.security.authorization": "false",
            "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
        },
        jars=None,
        py_files="/opt/airflow/src.zip",
        driver_memory="2g",
        executor_memory="2g",
        executor_cores=1,
        num_executors=1,
    )

    bronze_to_silver
