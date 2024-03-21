from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,  
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        dag_id='example_spark_dag',
        default_args=default_args,
        schedule_interval=None, 
        start_date=datetime(2024, 3, 18),  
        catchup=False,
        tags=['example', 'spark']
) as dag:

    submit_job = SparkSubmitOperator(
        application="/opt/spark/app/firstsparkapp.py",  # Replace with your Spark application
        task_id="submit_spark_job",
        conn_id="spark_default",  # Ensure this connection exists in Airflow UI
        conf={"spark.master":"spark://spark:7077"}  # Adjust config as needed 

    )

submit_job