# dags/amazon_reviews_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipeline_functions import download_to_gcs, transform_to_valid_json, decompress_large_file, load_json_to_bigquery
from google.cloud import bigquery

BUCKET_NAME = "interview-task-fd033c3b"
PROJECT_ID = "amazon-reviews-project-465010"
DATASET_ID = "stg_sample01"
METADATA_SCHEMA = [
    bigquery.SchemaField("asin",        "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title",       "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("price",       "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("brand",       "STRING", mode="NULLABLE"),
    bigquery.SchemaField("imUrl",       "STRING", mode="NULLABLE"),
    bigquery.SchemaField("categories",  "JSON",   mode="NULLABLE"),
    bigquery.SchemaField("salesRank",   "JSON",   mode="NULLABLE"),
    bigquery.SchemaField("related",     "JSON",   mode="NULLABLE"),
]


default_args = {
    'owner': 'sajad',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 6),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'amazon_reviews_sample_pipeline',
    default_args=default_args,
    description='Process sample Amazon review files',
    schedule_interval=None,
    catchup=False,
)

download_metadata_file = PythonOperator(
    task_id='download_metadata_file',
    python_callable=download_to_gcs,
    op_kwargs={
        'url': 'https://storage.googleapis.com/interview-task-fd033c3b/samples/metadata_sample.json.gz',
        'gcs_path': f'gs://{BUCKET_NAME}/extracted_files/metadata_sample.json.gz'
    },
    dag=dag,
)

download_reviews_file = PythonOperator(
    task_id='download_reviews_file',
    python_callable=download_to_gcs,
    op_kwargs={
        'url': 'https://storage.googleapis.com/interview-task-fd033c3b/samples/item_dedup_sample.json.gz',
        'gcs_path': f'gs://{BUCKET_NAME}/extracted_files/item_dedup_sample.json.gz'
    },
    dag=dag,
)

# transform metadata
transform_metadata = PythonOperator(
    task_id='transform_metadata',
    python_callable=transform_to_valid_json,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/extracted_files/metadata_sample.json.gz',
        'output_gcs': f'gs://{BUCKET_NAME}/processed/metadata_sample.jsonl'
    },
    dag=dag,
)

transform_reviews = PythonOperator(
    task_id='transform_reviews',
    python_callable=decompress_large_file,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/extracted_files/item_dedup_sample.json.gz',
        'output_gcs': f'gs://{BUCKET_NAME}/processed/item_dedup_sample.jsonl'
    },
    dag=dag,
)


load_metadata_to_bq = PythonOperator(
    task_id='load_metadata_to_bigquery',
    python_callable=load_json_to_bigquery,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/processed/metadata_sample.jsonl',
        'output_table': f'{PROJECT_ID}.{DATASET_ID}.metadata_sample_airflow_test01',
        'schema': METADATA_SCHEMA
    },
    dag=dag,
)

load_reviews_to_bq = PythonOperator(
    task_id='load_reviews_data_to_bigquery',
    python_callable=load_json_to_bigquery,
    op_kwargs={
        'input_gcs': f'gs://{BUCKET_NAME}/processed/item_dedup_sample.jsonl',
        'output_table': f'{PROJECT_ID}.{DATASET_ID}.item_dedup_sample_airflow_test_01'
    },
    dag=dag,

)


# running parallaly
download_metadata_file >> transform_metadata >> load_metadata_to_bq
download_reviews_file >> transform_reviews >> load_reviews_to_bq
