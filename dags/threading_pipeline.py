# dags/amazon_reviews_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from pipeline_functions import download_to_gcs, transform_to_valid_json, decompress_large_file, load_json_to_bigquery
from google.cloud import bigquery

BUCKET_NAME = "interview-task-fd033c3b"
PROJECT_ID = "amazon-reviews-project-465010"
DATASET_ID = "dbt_staging_landing_zone"
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
    'threading_pipeline',
    default_args=default_args,
    description='uses threading',
    schedule_interval=None,
    catchup=False,
)


# transform_reviews = PythonOperator(
#     task_id='transform_reviews',
#     python_callable=decompress_threading,
#     op_kwargs={
#         'input_gcs': f'gs://{BUCKET_NAME}/extracted_files/item_dedup.json.gz',
#         'output_gcs': f'gs://{BUCKET_NAME}/processed/item_dedup.jsonl'
#     },
#     dag=dag,
# )


# load_reviews_to_bq = PythonOperator(
#     task_id='load_reviews_data_to_bigquery',
#     python_callable=load_json_to_bigquery,
#     op_kwargs={
#         'input_gcs': f'gs://{BUCKET_NAME}/processed/item_dedup.jsonl',
#         'output_table': f'{PROJECT_ID}.{DATASET_ID}.items_dedup'
#     },
#     dag=dag,

# )

download_metadata_file = PythonOperator(
    task_id='download_metadata_file',
    python_callable=download_to_gcs,
    op_kwargs={
        'url': 'https://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz',
        'gcs_path': f'gs://{BUCKET_NAME}/extracted_files/metadata.json.gz'
    },
    dag=dag,
)
