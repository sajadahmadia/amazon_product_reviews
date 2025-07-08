# GCP config
BUCKET_NAME = "interview-task-fd033c3b"
PROJECT_ID = "amazon-reviews-project-465010"
DATASET_ID = "dbt_staging_landing_zone"

# urls
METADATA_URL = "https://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz"
REVIEWS_URL = "https://snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz"

# processing config
CHUNK_SIZE = 10 * 1024 * 1024  # 10mb
DOWNLOAD_TIMEOUT = 30
LOG_INTERVAL = 100000  # log every 100k lines

# paths
EXTRACTED_PATH = "extracted_files"
PROCESSED_PATH = "processed"
