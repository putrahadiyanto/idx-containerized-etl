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
    image='etl-extract-service',
    command=['python', 'extract.py'],
    network_mode='host',
    dag=dag
)

# Transform task - Processing data using PySpark
transform_task = DockerOperator(
    task_id='transform_data_using_pyspark',
    image='etl-transform-service',
    command=['python', 'transform.py'],
    network_mode='host',
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
    task_id='load_data_into_mongodb',
    image='etl-load-service',
    command=['python', 'load.py'],
    network_mode='host',
    mounts=[
        {
            'source': 'etl-shared-data',
            'target': '/data',
            'type': 'volume'
        }
    ],
    dag=dag
)

# Define task dependencies
extract_task >> transform_task >> load_task
