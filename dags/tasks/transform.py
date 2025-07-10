from google.cloud import storage
import logging
import gzip
import os
import time
import json
import ast
import tempfile


def decompress_gzip_file(input_gcs: str, output_gcs: str, buffer_size: int = 1024 * 1024):
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
