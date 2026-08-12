import time
import logging
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param

# Default args match Kestra's pluginDefaults for logging/retries
default_args = {
    "owner": "zoomcamp",
}

@dag(
    dag_id="01_hello_world",
    schedule_interval="0 10 * * *",  # Trigger schedule (10:00 AM daily)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,    # Disabled trigger concept from Kestra
    max_active_runs=2,               # Concurrency limit: 2
    default_args=default_args,
    # Inputs concept via Airflow Params
    params={
        "name": Param("Will", type="string", description="Name to greet"),
    },
    tags=["zoomcamp"],
)
def hello_world_workflow():

    @task
    def hello_message(**context):
        # Accessing runtime parameter
        name = context["params"]["name"]
        welcome_message = f"Hello, {name}!"
        
        # Demonstrating pluginDefaults: level ERROR logging
        logging.error(welcome_message)

    @task
    def generate_output() -> str:
        # Returning a string automatically stores it in Airflow XCom
        return "I was generated during this workflow."

    @task
    def sleep_task():
        # PT15S sleep concept
        time.sleep(15)

    @task
    def log_output(output_value: str):
        # Reading output from generate_output task via TaskFlow API
        logging.error(f"This is an output: {output_value}")

    @task
    def goodbye_message(**context):
        name = context["params"]["name"]
        logging.error(f"Goodbye, {name}!")

    # Set up task execution sequence (downstream dependencies)
    hello = hello_message()
    gen_out = generate_output()
    slp = sleep_task()
    log_out = log_output(gen_out)
    goodbye = goodbye_message()

    # Explicit ordering: hello -> generate_output -> sleep -> log_output -> goodbye
    hello >> gen_out >> slp >> log_out >> goodbye

hello_world_workflow()