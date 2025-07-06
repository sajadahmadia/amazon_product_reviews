# Dockerfile
FROM apache/airflow:2.8.0

USER root
RUN apt-get update && apt-get install -y gcc python3-dev

USER airflow

# Install your dependencies
RUN pip install --no-cache-dir \
    google-cloud-storage==2.10.0 \
    google-cloud-bigquery==3.11.4 \
    requests==2.31.0 \
    retrying==1.3.4 \
    pandas==2.0.3