# How to this project
Instructions on how to run this project: 

To implement the following steps, you need to be in the root folder of the current directory.
## Steps
1. create a python environment using python3.11 -m venv .venv and activate it source .venv/bin/activate
2. install the required libraries using pip install -r requirements.txt
3. you need to have access to a GCP account. create a new project and give it a name.
4. go to IAM & Admins >> Service Accounts of your GCP project, and create a new service account with the following roles:
    - Bigquery data editor
    - Bigquery job user
    - Storage Admin
    - Storage object admin
5. while in the service account, create a new key, it automatically downloads a file into your local.
    - Note: when creating resources in GCP, ensure bigquery datasets and GCS buckets are in the EU region to match the DAG configuration.
6. rename the file as service-account.json (it's already a json), and put it under `./keys/service-account.json` of the project's root directory.
    - in the future, our local airflow will use this file to run actions on our GCP account
7. while in the root of the project, create a new dbt project (in the cli, type `dbt init amazon_reviews_dbt`, don't change the name, it will be used by docker file)
    - when questions prompted, answer:
        * type: [1] bigquery
        * Authentication: service-account
        * Path: ../keys/service-account.json
        * Project: your-gcp-project-id
        * Dataset: dbt_dev
        * Location: EU
8. to make sure your dbt project is healthy, `cd amazon_reviews_dbt` and then run `dbt debug`. you should see "All checks passed!" message. 
    - if not, have a look at your profiles.yml file using `nano ~/.dbt/profiles.yml` and see if the details match your database connection details and also the details of your proejct's yaml file at: `./amazon_reviews_dbt/dbt_project.yml`
8. now, we want to create the airflow project. first, start docker desktop on your machine
9. while you're in the root of the project, in your cli, run: `docker-compose up -d` 
    - it will set up 4 services in your local, a postgres database, and 3 other services for airflow (scheduler, webserver, init). all of these services, are built upon a docker image given at `Dockerfile` 
10. wait for the services to run, you should see their names with green checkmarks in front of them. check if they're active using:
    - `docker compose ps`
11. you can access your airflow web ui via your localhost on port 8080.
    - username: sajad password: admin (you can change them in the docker compose file)
    - if your port 8080 is already engaged with another service, kill the other program or change the port mapping in the docker compose file
12. Provisioning: you need to provision some resources before being able to run the airflow dags. 
    - create a GCS bucket. then, in the `./dags/config/settings.py` file, set the bucket_name, dataset_name, and project_id to comply with your resources. optional to change extracted_path and processed_path. the only resource that needs to be create beforehand is the bucket_name.
    - ensure all resources are created in EU
13. you can manually run the given pipelines and see the results in your GCP account. 
    - run the main pipeline in `./dags/main_pipeline_local.py` . it will create the seeds for the dbt local development environment
    - run the sample category pipeline at `./dags/sample_toys_category_pipeline.py`. it will create the seeds for the dbt staging environment

## Possible Errors and Solutions:
- If DAGs don't appear in Airflow ui: wait 30 seconds for Airflow to scan the dags folder
- If bigquery jobs fail: check that all resources are in EU region
- If "All checks passed!" fails when you run dbt debug: verify service account has all required permissions
- always read error messages completely!