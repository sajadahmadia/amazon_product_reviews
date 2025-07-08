from google.cloud import bigquery

METADATA_SCHEMA = [
    bigquery.SchemaField("asin", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("imUrl", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("categories", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("salesRank", "JSON", mode="NULLABLE"),
    bigquery.SchemaField("related", "JSON", mode="NULLABLE"),
]
