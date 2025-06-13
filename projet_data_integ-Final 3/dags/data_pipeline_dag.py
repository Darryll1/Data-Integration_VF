from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 11),
    'retries': 1,
}

with DAG(
    dag_id='pipeline_population_data',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    description='Pipeline Kafka -> Transformation -> Cube',
) as dag:

    start_producer = BashOperator(
        task_id='start_producer',
        bash_command='python /app/population_survey_project/kafka_producer/producer.py'
    )

    hdfs_transform = BashOperator(
        task_id='transform_hdfs',
        #bash_command='docker exec spark-master spark-submit /app/population_survey_project/hdfs_data/hdfs_reader.py'
        bash_command='python /app/population_survey_project/hdfs_data/hdfs_reader.py'
    )

    start_consumer = BashOperator(
        task_id='start_consumer',
        bash_command='python /app/population_survey_project/spark_streaming/consumer.py'
    )

    compute_cube = BashOperator(
        task_id='compute_cube',
        bash_command='python /app/population_survey_project/spark_streaming/cube_population.py'
    )

    
    [start_producer, hdfs_transform, start_consumer, compute_cube]
   
