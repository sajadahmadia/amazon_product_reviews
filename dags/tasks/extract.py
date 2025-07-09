from retrying import retry
from google.cloud import storage
import requests
import logging
import time



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