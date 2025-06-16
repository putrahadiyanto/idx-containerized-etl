"""
IDX Laporan Keuangan ETL DAG - Containerized Version

This DAG implements the main ETL process for IDX financial data.
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
    'idx_lapkeu_etl_containerized',
    default_args=default_args,
    description='IDX Laporan Keuangan ETL process using containers',
    schedule_interval='0 0 1 2,5,8,11 *',  # Run on 1st of Feb, May, Aug, Nov
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=['idx', 'financial', 'etl', 'containerized']
)

# Extract task - Scrapping data from IDX
extract_task = DockerOperator(
    task_id='scrapping_data_idx',
    image='idx-etl-extract:latest',
    command=['python', 'extract.py'],
    network_mode='bridge',
    auto_remove=True,  # Automatically remove container after completion
    environment={
        'MONGO_HOST': 'host.docker.internal',
        'MONGO_PORT': '27017',
        'MONGO_USERNAME': 'root',
        'MONGO_PASSWORD': 'password',
        'MONGO_AUTH_DB': 'admin',
        'MONGO_DATABASE': 'idx_data'
    },
    dag=dag
)

# Transform task - Processing data using PySpark
transform_task = DockerOperator(
    task_id='transform_data_using_pyspark',
    image='idx-etl-transform:latest',
    command=['python', 'transform.py'],
    network_mode='etl_etl-network',
    auto_remove=True,  # Automatically remove container after completion
    environment={
        'MONGO_HOST': 'host.docker.internal',
        'MONGO_PORT': '27017',
        'MONGO_USERNAME': 'root',
        'MONGO_PASSWORD': 'password',
        'MONGO_AUTH_DB': 'admin',
        'MONGO_DATABASE': 'idx_data'
    },
    mounts=[
        {
            'source': 'etl-shared-data',
            'target': '/data',
            'type': 'volume'
        }
    ],
    dag=dag
)

# Load task - Loading data into MongoDB
load_task = DockerOperator(
    task_id='load_data_into_mongodb',    image='idx-etl-load:latest',
    command=['python', 'load.py'],
    network_mode='etl_etl-network',
    auto_remove=True,  # Automatically remove container after completion
    environment={
        'MONGO_HOST': 'mongodb',
        'MONGO_PORT': '27017',
        'MONGO_USERNAME': 'root',
        'MONGO_PASSWORD': 'password',
        'MONGO_AUTH_DB': 'admin',
        'MONGO_DATABASE': 'idx_data_final'
    },
    mounts=[
        {
            'source': 'etl-shared-data',
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
