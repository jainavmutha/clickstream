from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 1),
    "retries": 1,
}

dag = DAG(
    "clickstream_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
)

start_kafka_producer = BashOperator(
    task_id="start_kafka_producer",
    bash_command="python /path/to/kafka/producer.py",
    dag=dag,
)

start_spark_streaming = BashOperator(
    task_id="start_spark_streaming",
    bash_command="spark-submit /path/to/spark/clickstream_processor.py",
    dag=dag,
)

start_kafka_producer >> start_spark_streaming