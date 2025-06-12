"""
YFinance ETL DAG - Containerized Version

This DAG implements the main ETL process for YFinance stock data.
It uses existing containerized services: Extract, Transform, and Load.
"""

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Set default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 1),
}

# Define the DAG
dag = DAG(
    'yfinance_etl_containerized',
    default_args=default_args,
    description='YFinance stock data ETL process using containers',
    schedule_interval='0 2 * * 1-5',  # Run at 2 AM Monday to Friday (weekdays)
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['yfinance', 'stock', 'etl', 'containerized']
)

# Extract task - Extracting stock data from YFinance
extract_task = DockerOperator(
    task_id='extract_yfinance_data',
    image='yfinance-extract-service',
    command=['python', 'yfinance_extractor.py'],
    network_mode='host',
    auto_remove=True,  # Automatically remove container after completion
    dag=dag
)

# Transform task - Processing stock data using PySpark
transform_task = DockerOperator(
    task_id='transform_stock_data_pyspark',
    image='yfinance-transform-service',
    command=['python', 'mongo_to_spark.py'],
    network_mode='host',
    auto_remove=True,  # Automatically remove container after completion
    mounts=[
        {
            'source': 'transform_shared-data',
            'target': '/data',
            'type': 'volume'
        }
    ],
    dag=dag
)

# Load task - Loading transformed data into MongoDB
load_task = DockerOperator(
    task_id='load_stock_data_mongodb',
    image='yfinance-load-service',
    command=['python', 'load_json_to_mongo.py'],
    network_mode='host',
    auto_remove=True,  # Automatically remove container after completion
    mounts=[
        {
            'source': 'transform_shared-data',
            'target': '/data',
            'type': 'volume'
        }
    ],
    dag=dag
)

# Cleanup task - Remove any orphaned containers and unused images
cleanup_task = DockerOperator(
    task_id='cleanup_containers',
    image='docker:latest',
    command=[
        'sh', '-c', 
        'docker container prune -f && docker image prune -f --filter label=stage=builder'
    ],
    network_mode='host',
    auto_remove=True,
    mounts=[
        {
            'source': '/var/run/docker.sock',
            'target': '/var/run/docker.sock',
            'type': 'bind'
        }
    ],
    dag=dag
)

# Define task dependencies
extract_task >> transform_task >> load_task >> cleanup_task
