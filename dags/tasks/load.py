from google.cloud import bigquery
import logging


def load_json_to_bigquery(input_gcs, output_table, schema=None):
    """a simple function to load results to bigquery from a gcs
    OPTIMIZATION 1: allow some bad records to prevent full job failure
    OPTIMIZATION 2: schema provided to skip inference which makes the job faster in large files
    """
    # loading items into bigquery
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        max_bad_records=10,
        ignore_unknown_values=True
    )

    if schema:
        job_config.schema = schema
        job_config.autodetect = False
    else:
        job_config.autodetect = True

    load_job = client.load_table_from_uri(
        input_gcs,
        output_table,
        job_config=job_config
    )
    try:
        logging.info(f"starting job {load_job.job_id}")
        load_job.result()
        result = {
            'status': 'success',
            'rows_loaded': load_job.output_rows,
            'bytes_processed': load_job.input_file_bytes,
            'bad_records': load_job.error_result.errors if load_job.error_result else [],
            'output_table': output_table}

        logging.info(
            f"loaded {load_job.output_rows:,} rows into {output_table}")
        return result
    except Exception as e:
        logging.exception(f'unexpected error happened: {e}')
        raise
