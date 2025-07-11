# Amazon Reviews Data Pipeline

A data engineering pipeline that processes Amazon product reviews and metadata using Airflow, dbt, and BigQuery.

## Quick Start
1. Set up GCP project with BigQuery and Storage permissions
2. Place service account key in `./keys/service-account.json`
3. Run `docker-compose up -d` to start Airflow
4. Access Airflow UI at `localhost:8080` (username: sajad, password: admin)
5. Run DAGs to process data and execute dbt transformations

See `HOW_TO_RUN.md` for detailed setup instructions.