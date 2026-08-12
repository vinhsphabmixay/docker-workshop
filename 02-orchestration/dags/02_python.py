import logging
from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="02_python",
    description="Fetches Docker Hub download metrics using Python and requests.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["zoomcamp"],
)
def python_workflow():

    # Option 1: Standard PythonTask (runs directly inside the Airflow worker)
    @task
    def collect_stats() -> dict:
        import requests

        image_name = "apache/airflow"
        url = f"https://hub.docker.com/v2/repositories/{image_name}/"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        downloads = data.get("pull_count", "Not available")
        logging.info(f"Docker Hub Pull Count for {image_name}: {downloads}")
        
        # In Airflow, returning a dict automatically acts as outputs (stored in XCom)
        return {"downloads": downloads}

    # Option 2: Docker-isolated execution (Direct equivalent to Kestra's Docker taskRunner)
    # Uncomment and use this task if you require isolated container runtime with custom image/dependencies:
    """
    from airflow.providers.docker.operators.docker import DockerOperator

    collect_stats_docker = DockerOperator(
        task_id="collect_stats_docker",
        image="python:3.11-slim",
        command='python -c "'
                'import requests; '
                'r = requests.get(\'https://hub.docker.com/v2/repositories/apache/airflow/\'); '
                'print(r.json().get(\'pull_count\'))"',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )
    """

    collect_stats()

python_workflow()