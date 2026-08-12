from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="07_gcp_setup",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["zoomcamp", "gcp"],
) as dag:

    GCP_AUTH_CMD = "gcloud auth activate-service-account --key-file=/opt/airflow/keys/gcp-key.json"

    # 1. Create GCS Bucket using gcloud CLI
    create_gcs_bucket = BashOperator(
        task_id="create_gcs_bucket",
        bash_command=f"""
        {GCP_AUTH_CMD}
        gcloud storage buckets create gs://{{{{ var.value.GCP_BUCKET_NAME }}}} \
            --project={{{{ var.value.GCP_PROJECT_ID }}}} \
            --location={{{{ var.value.GCP_LOCATION }}}} \
            --default-storage-class=REGIONAL || true
        """,
    )

    # 2. Create BigQuery Dataset using bq CLI
    create_bq_dataset = BashOperator(
        task_id="create_bq_dataset",
        bash_command=f"""
        {GCP_AUTH_CMD}
        bq show --project_id={{{{ var.value.GCP_PROJECT_ID }}}} {{{{ var.value.GCP_DATASET }}}} > /dev/null 2>&1 || \
        bq mk -d -f --location={{{{ var.value.GCP_LOCATION }}}} {{{{var.value.GCP_PROJECT_ID}}}}:{{{{ var.value.GCP_DATASET }}}}
        """,
    )

    create_gcs_bucket >> create_bq_dataset