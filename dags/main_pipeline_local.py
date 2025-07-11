# dags/amazon_reviews_pipeline.py
from datetime import datetime, timedelta
from pytz import timezone
from airflow import DAG
# importing the operators
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# importing the custom functions
from tasks.extract import download_to_gcs
from tasks.transform import transform_to_valid_json, decompress_gzip_file
from tasks.load import load_json_to_bigquery
# importing the constants
from config.settings import PROJECT_ID, DATASET_ID, BUCKET_NAME, EXTRACTED_PATH, PROCESSED_PATH, METADATA_URL, REVIEWS_URL
from config.schemas import METADATA_SCHEMA


default_args = {
    'owner': 'sajad',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 10, 8, 0, 0, tzinfo=timezone('Europe/Amsterdam')),
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'amazon_reviews_main_files_local',
    default_args=default_args,
    description='process the main large files on local',
    schedule_interval='0 8 * * *',  # to run everyday at 8 am amsterdam time
    catchup=False,
    max_active_runs=1
)


create_dataset = BigQueryInsertJobOperator(
    task_id='create_dataset',
    configuration={
        "query": {
            "query": f"CREATE SCHEMA IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}`",
            "useLegacySql": False
        }
    },
    project_id=PROJECT_ID,
    location='EU',
    dag=dag,
)

download_metadata_file = PythonOperator(
    task_id='download_metadata_file',
    python_callable=download_to_gcs,
    op_kwargs={
        'url': METADATA_URL,
        'gcs_path': f'gs://{BUCKET_NAME}/{EXTRACTED_PATH}/metadata.json.gz'
    },
    dag=dag,
)

download_reviews_file = PythonOperator(
    task_id='download_reviews_file',
    python_callable=download_to_gcs,
    op_kwargs={
        'url': REVIEWS_URL,
        'gcs_path': f'gs://{BUCKET_NAME}/{EXTRACTED_PATH}/item_dedup.json.gz'
    },
    dag=dag,
)


transform_metadata = PythonOperator(
    task_id='transform_metadata',
    python_callable=transform_to_valid_json,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/{EXTRACTED_PATH}/metadata.json.gz',
        'output_gcs': f'gs://{BUCKET_NAME}/{PROCESSED_PATH}/metadata.jsonl'
    },
    dag=dag,
    sla=timedelta(minutes=45)
)

transform_reviews = PythonOperator(
    task_id='transform_reviews',
    python_callable=decompress_gzip_file,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/{EXTRACTED_PATH}/item_dedup.json.gz',
        'output_gcs': f'gs://{BUCKET_NAME}/{PROCESSED_PATH}/item_dedup.jsonl'
    },
    dag=dag,
    sla=timedelta(minutes=30)
)


load_metadata_to_bq = PythonOperator(
    task_id='load_metadata_to_bigquery',
    python_callable=load_json_to_bigquery,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/{PROCESSED_PATH}/metadata.jsonl',
        'output_table': f'{PROJECT_ID}.{DATASET_ID}.metadata',
        'schema': METADATA_SCHEMA
    },
    dag=dag,
)

load_reviews_to_bq = PythonOperator(
    task_id='load_reviews_data_to_bigquery',
    python_callable=load_json_to_bigquery,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/{PROCESSED_PATH}/item_dedup.jsonl',
        'output_table': f'{PROJECT_ID}.{DATASET_ID}.items_dedup'
    },
    dag=dag,

)

run_dbt_build = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /opt/airflow/dbt && dbt build --profiles-dir docker',
    dag=dag,
    sla=timedelta(minutes=10)
)


# running in parallel
create_dataset >> [download_metadata_file, download_reviews_file]

download_metadata_file >> transform_metadata >> load_metadata_to_bq
download_reviews_file >> transform_reviews >> load_reviews_to_bq

[load_metadata_to_bq, load_reviews_to_bq] >> run_dbt_build
