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
import uuid


@retry(
    stop_max_attempt_number=5,
    wait_exponential_multiplier=1000,
    wait_exponential_max=60000
)
def download_to_gcs(url: str, gcs_path: str, chunk_size: int = 100 * 1024 * 1024, timeout: int = 30) -> dict:
    """
    Download large files to GCS with automatic retry

    OPTIMIZATION 1: exponential retries to prevent multiple calls in row
    OPTIMIZATION 2: chunked streaming for memory efficiency
    """
    start_time = time.time()
    try:
        logging.info(f"establishing connection to {gcs_path}")
        storage_client = storage.Client()
        blob = storage.Blob.from_string(gcs_path, client=storage_client)
    except Exception as e:
        logging.exception(
            f"failed to connect to the blob at path: {gcs_path}, reason: {e}")
        raise

    try:
        logging.info(f"establishing connection to url: {url}")
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logging.error(
            f"timeout connecting to {url} after {timeout}. you can possibly increase timeout")
        raise
    except requests.exceptions.HTTPError as e:
        logging.error(f"got HTTP error {response.status_code}: {e}")
        raise
    except requests.exceptions.RequestException as e:
        logging.error(f"failed to connect to url: {e}")
        raise

    counter = 0
    read_size = 0
    try:
        with blob.open('wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    counter += 1
                    read_size += len(chunk)
                    logging.info(
                        f"read chunk number {counter} | size read up to now: {read_size/1024/1024:,.1f} mb")
                    f.write(chunk)
    except Exception as e:
        logging.error(
            f"failed during download after {read_size/1024/1024:,.1f} mb: {e}")
        raise

    elapsed_time = time.time() - start_time
    result = {
        'status': 'success',
        'download_size_in_mb': read_size/1024/1024,
        'chunks_processed': counter,
        'elapsed_seconds': elapsed_time,
        'gcs_path': gcs_path
    }

    logging.info(
        f"downlaod completed: {read_size/1024/1024:,.1f} mb in {elapsed_time:.1f}s")
    return result


def decompress_large_file(input_gcs: str, output_gcs: str, buffer_size: int = 1024 * 1024):
    """since we can't import compressed gz files larger than 4GB into bigquery, we need to decompress them.
    OPTIMIZATION 1: use of streaming files, no pressure on memory
    OPTIMIZATON 2: direct gcs to gcs transfer, no intermediary writes
    """

    storage_client = storage.Client()

    try:
        input_blob = storage.Blob.from_string(input_gcs, client=storage_client)
        output_blob = storage.Blob.from_string(
            output_gcs, client=storage_client)
    except Exception as e:
        logging.error(f"failed to connect to GCS: {e}")
        raise

    logging.info(f"decompressing {input_gcs} to {output_gcs}...")

   # streaming decompression
    lines_processed = 0
    start_time = time.time()
    try:
        with gzip.open(input_blob.open('rb'), 'rt', encoding='utf-8') as fin, \
                output_blob.open('w', encoding='utf-8', chunk_size=buffer_size) as fout:

            # write line by line
            for i, line in enumerate(fin):
                fout.write(line)
                lines_processed = i + 1
                if i % 100000 == 0:
                    logging.info(f"processed {i} lines...")
    except UnicodeDecodeError as e:
        logging.error(f"encoding error at line {lines_processed}: {e}")
        raise
    except Exception as e:
        logging.error(
            f"failed during decompression after {lines_processed:,} lines: {e}")
        raise
    elapsed_time = time.time() - start_time
    result = {
        'status': 'success',
        'lines_processed': lines_processed,
        'elapsed_seconds': elapsed_time,
        'output_path': output_gcs
    }
    logging.info(
        f"decompression completed {lines_processed:,} in {elapsed_time} seconds")
    return result


def transform_to_valid_json(input_gcs: str, output_gcs: str, bad_records_threshold: int = 10):
    """a big function to address the problem of the metadata file. 
    OPTIMIZATION 1: processes lines in a streaming manner, not to load the whole file into memory in one shot
    OPTIMIZATION 2: use of a temp file (on disk) which is faster than network call for each line to gcs, at the end, we have only 1 network call to 
                    upload the temp file
    """
    storage_client = storage.Client()
    temp_path = None

    try:
        input_blob = storage.Blob.from_string(input_gcs, client=storage_client)
        output_blob = storage.Blob.from_string(
            output_gcs, client=storage_client)
    except Exception as e:
        logging.error(f"failed to connect to GCS: {e}")
        raise

    parse_time = 0.0
    write_time = 0.0
    start_total = time.time()
    line_count = 0
    parse_errors = 0

    # use of a temp file
    try:
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl') as temp_file:
            temp_path = temp_file.name

            # process to local file
            logging.info("procesing file...")
            try:
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
                                parse_errors += 1
                                if parse_errors <= bad_records_threshold:
                                    logging.warning(
                                        f"failed to parse line {line} ")
                                continue
                        parse_time += time.time() - start_parse

                        start_write = time.time()
                        temp_file.write(json.dumps(
                            record, ensure_ascii=False) + '\n')
                        write_time += time.time() - start_write

                        line_count += 1
                        if line_count % 50000 == 0:
                            elapsed = time.time() - start_total
                            logging.info(
                                f"lines: {line_count} , total time passed: {elapsed:.1f}s , parsing time: {parse_time:.1f}s , writing time: {write_time:.1f}s")
            except UnicodeDecodeError as e:
                logging.exception(f"encoding error at line {line_count}: {e}")
                raise
            except Exception as e:
                logging.exception(
                    f"unexpected error at line {line_count}: {e}")
                raise

        # now, we can upload our file
        try:
            logging.info(
                f"uploading {os.path.getsize(temp_path)/1e9:.1f} GB to GCS")
            output_blob.upload_from_filename(temp_path)
        except Exception as e:
            logging.exception(f"failed at uploaded blob to {output_gcs}: {e}")
            raise
    # resource cleanup,since we set the temp file not to be deleted with context manager
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)

    result = {
        'status': 'success',
        'valid_records': line_count,
        'parse_errors': parse_errors,
        'parse_time_seconds': parse_time,
        'write_time_seconds': write_time,
        'output_path': output_gcs
    }
    logging.info("operation completed successfully")
    return result


def load_json_to_bigquery(input_gcs, output_table, schema=None):
    """a simple function to load results to bigquery from a gcs
    OPTIMIZATION a: allow some bad records to prevent full job failure
    OPTIMIZATION 3: schema provided to skip inference which makes the job faster in large files
    """
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
