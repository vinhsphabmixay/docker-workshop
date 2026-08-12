from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

with DAG(
    dag_id="07_gcp_setup",
    schedule=None,
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=['zoomcamp', 'kv']
) as dag:

    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name="{{var.value.GCP_BUCKET_NAME}}",
        project_id="{{var.value.GCP_PROJECT_ID}}",
        location="{{var.value.GCP_LOCATION}}",
        storage_class="REGIONAL",
        gcp_conn_id="google_cloud_default"
    )

    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id="{{var.value.GCP_DATASTE}}",
        project_id="{{var.value.GCP_PROJECT_ID}}",
        location="{{var.value.GCP_LOCATION}}",
        exists_ok=True,
        gcp_conn_id="google_cloud_default"
    )

    create_gcs_bucket >> create_bq_dataset