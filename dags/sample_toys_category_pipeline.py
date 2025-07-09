# dags/amazon_reviews_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from config.settings import PROJECT_ID, DATASET_ID, BUCKET_NAME, EXTRACTED_PATH, PROCESSED_PATH, SAMPLE_TOYS_METADTA_URL, SAMPLE_TOYS_REVIEWS_URL
from config.schemas import METADATA_SCHEMA
from tasks.extract import download_to_gcs
from tasks.transform import transform_to_valid_json, decompress_large_file
from tasks.load import load_json_to_bigquery


default_args = {
    'owner': 'sajad',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 6),
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'amazon_reviews_category_sample',
    default_args=default_args,
    description='Process sample Amazon review files',
    schedule_interval=None,
    catchup=False,
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
        'url': SAMPLE_TOYS_METADTA_URL,
        'gcs_path': f'gs://{BUCKET_NAME}/{EXTRACTED_PATH}/meta_Toys_and_Games.json.gz'
    },
    dag=dag,
)

download_reviews_file = PythonOperator(
    task_id='download_reviews_file',
    python_callable=download_to_gcs,
    op_kwargs={
        'url': SAMPLE_TOYS_REVIEWS_URL,
        'gcs_path': f'gs://{BUCKET_NAME}/{EXTRACTED_PATH}/reviews_Toys_and_Games.json.gz'
    },
    dag=dag,
)


transform_metadata = PythonOperator(
    task_id='transform_metadata',
    python_callable=transform_to_valid_json,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/{EXTRACTED_PATH}/meta_Toys_and_Games.json.gz',
        'output_gcs': f'gs://{BUCKET_NAME}/{PROCESSED_PATH}/meta_Toys_and_Games.jsonl'
    },
    dag=dag,
)

transform_reviews = PythonOperator(
    task_id='transform_reviews',
    python_callable=decompress_large_file,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/{EXTRACTED_PATH}/reviews_Toys_and_Games.json.gz',
        'output_gcs': f'gs://{BUCKET_NAME}/{PROCESSED_PATH}/reviews_Toys_and_Games.jsonl'
    },
    dag=dag,
)


load_metadata_to_bq = PythonOperator(
    task_id='load_metadata_to_bigquery',
    python_callable=load_json_to_bigquery,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/{PROCESSED_PATH}/meta_Toys_and_Games.jsonl',
        'output_table': f'{PROJECT_ID}.{DATASET_ID}.metadata',
        'schema': METADATA_SCHEMA
    },
    dag=dag,
)

load_reviews_to_bq = PythonOperator(
    task_id='load_reviews_data_to_bigquery',
    python_callable=load_json_to_bigquery,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/processed/reviews_Toys_and_Games.jsonl',
        'output_table': f'{PROJECT_ID}.{DATASET_ID}.items_dedup'
    },
    dag=dag,

)

run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir docker',
    dag=dag,
)

run_dbt_test = BashOperator(
    task_id='run_dbt_test',
    bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir docker',
    dag=dag,
)


# running parallaly
create_dataset >> download_metadata_file >> transform_metadata >> load_metadata_to_bq
create_dataset >> download_reviews_file >> transform_reviews >> load_reviews_to_bq
[load_metadata_to_bq, load_reviews_to_bq] >> run_dbt_models >> run_dbt_test
