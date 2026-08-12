from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable

@dag(
    dag_id="06_gcp_kv",
    schedule=None,
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=['zoomcamp', 'kv']
)

def gcp_kv_pipeline():

    @task
    def set_gcp_project_id():
        Variable.set(key="GCP_PROJECT_ID", value="airflow-sandbox-505312")

    @task
    def set_gcp_location():
        Variable.set(key="GCP_LOCATION", value="europe-west1")

    @task
    def set_gcp_bucket_name():
        Variable.set(key="GCP_BUCKET_NAME", value="vinh-airflow")

    @task
    def set_gcp_dataset():
        Variable.set(key="GCP_DATASET", value="zoomcamp")

    (
        set_gcp_project_id()
        >> set_gcp_location()
        >> set_gcp_bucket_name()
        >> set_gcp_dataset()
    )

gcp_kv_dag = gcp_kv_pipeline()