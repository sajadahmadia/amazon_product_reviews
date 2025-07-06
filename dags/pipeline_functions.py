from retrying import retry
from google.cloud import storage, bigquery
import requests
import logging
import gzip
import os
import time
import json
import ast
import tempfile


@retry(
    stop_max_attempt_number=5,
    wait_exponential_multiplier=1000,
    wait_exponential_max=60000
)
def download_to_gcs(url: str, gcs_path: str, chunk_size: int = 10 * 1024 * 1024) -> None:
    """
    Download large files to GCS with automatic retry

    OPTIMIZATION: Exponential backoff prevents hammering the server
    OPTIMIZATION: Chunked streaming for memory efficiency
    """
    logging.info(f"establishing connection to {gcs_path}")
    storage_client = storage.Client()
    blob = storage.Blob.from_string(gcs_path, client=storage_client)

    logging.info(f"establishing connection to url: {url}")
    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()

    counter = 0
    read_size = 0
    with blob.open('wb') as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            counter += 1
            read_size += chunk_size
            logging.info(
                f"read chunk number {counter} | size read up to now: {read_size}")
            if chunk:
                f.write(chunk)

    logging.info(f"successfully downloaded {url} to {gcs_path}")


def decompress_large_file(input_gcs: str, output_gcs: str):
    storage_client = storage.Client()

    input_blob = storage.Blob.from_string(input_gcs, client=storage_client)
    output_blob = storage.Blob.from_string(output_gcs, client=storage_client)

    logging.info(f"decompressing {input_gcs} to {output_gcs}...")

   # streaming decompression
    with gzip.open(input_blob.open('rb'), 'rt', encoding='utf-8') as fin, \
            output_blob.open('w', encoding='utf-8') as fout:

        # write line by line
        for i, line in enumerate(fin):
            fout.write(line)
            if i % 100000 == 0:
                logging.info(f"processed {i} lines...")

    logging.info("decompression completed")


def transform_to_valid_json(input_gcs: str, output_gcs: str):
    storage_client = storage.Client()

    input_blob = storage.Blob.from_string(input_gcs, client=storage_client)
    output_blob = storage.Blob.from_string(output_gcs, client=storage_client)

    parse_time = 0.0
    write_time = 0.0
    start_total = time.time()
    line_count = 0.0

    # use of a temp file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl') as temp_file:
        temp_path = temp_file.name

        # Process to local file
        logging.info("rocessing file...")
        with gzip.open(input_blob.open('rb'), 'rt', encoding='utf-8') as fin:
            for line in fin:

                # here, do some cleaning
                start_parse = time.time()
                line = line.strip()
                if not line or line in ('[', ']', ','):
                    continue
                if line.endswith(','):
                    line = line[:-1]

                # main operation starts from here
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    try:
                        record = ast.literal_eval(line)
                    except (ValueError, SyntaxError):
                        continue
                parse_time += time.time() - start_parse

                start_write = time.time()
                temp_file.write(json.dumps(record, ensure_ascii=False) + '\n')
                write_time += time.time() - start_write

                line_count += 1
                if line_count % 50000 == 0:
                    elapsed = time.time() - start_total
                    logging.info(
                        f"lines: {line_count} , total time passed: {elapsed:.1f}s , parsing time: {parse_time:.1f}s , writing time: {write_time:.1f}s")

    # now, we can upload our file
    logging.info(
        f"uploading {os.path.getsize(temp_path)/1e9:.1f} GB to GCS...")
    output_blob.upload_from_filename(temp_path)

    # resource cleanup
    os.unlink(temp_path)
    logging.info("operation ompleted successfully")
    return True  # return true to make it explicit


def load_json_to_bigquery(input_gcs, output_table, schema=None):

    # loading items into bigquery
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
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
        logging.info(
            f"loaded {load_job.output_rows:,} rows into {output_table}")
        return True
    except Exception as e:
        logging.exception(f'unexpected error happened: {e}')
        raise
