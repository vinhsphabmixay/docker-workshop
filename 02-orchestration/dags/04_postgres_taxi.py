import gzip
import os
import requests
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "ny_taxi_postgres"  # Configure this connection in Airflow or set via env var

@dag(
    dag_id="04_postgres_taxi",
    description="Extracts NYC taxi data, streams into staging, and merges into main Postgres tables",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "taxi": Param("yellow", enum=["yellow", "green"], description="Select taxi type"),
        "year": Param("2019", enum=["2019", "2020"], description="Select year"),
        "month": Param("01", enum=[f"{i:02d}" for i in range(1, 13)], description="Select month"),
    },
    tags=["zoomcamp"],
)
def postgres_taxi_pipeline():

    @task
    def set_label(**context) -> dict:
        """Constructs variables equivalent to Kestra vars."""
        params = context["params"]
        taxi = params["taxi"]
        year = params["year"]
        month = params["month"]
        file_name = f"{taxi}_tripdata_{year}-{month}.csv"
        
        return {
            "taxi": taxi,
            "file_name": file_name,
            "staging_table": f"public.{taxi}_tripdata_staging",
            "table": f"public.{taxi}_tripdata",
            "url": f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{file_name}.gz"
        }

    @task
    def extract(meta: dict) -> str:
        """Downloads gzipped CSV and extracts it locally."""
        url = meta["url"]
        local_filepath = f"/tmp/{meta['file_name']}"
        
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with gzip.GzipFile(fileobj=response.raw) as gz, open(local_filepath, "wb") as out_file:
            out_file.write(gz.read())
            
        return local_filepath

    @task.branch
    def branch_by_taxi_type(meta: dict) -> str:
        """Routes execution path based on selected taxi type (If task in Kestra)."""
        if meta["taxi"] == "yellow":
            return "yellow_workflow.create_tables"
        return "green_workflow.create_tables"

    # --- YELLOW TAXI PIPELINE ---
    @task(task_id="yellow_workflow.create_tables")
    def yellow_create_tables(meta: dict):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        table = meta["table"]
        staging = meta["staging_table"]
        
        schema_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            unique_row_id text
            , filename text
            , VendorID text
            , tpep_pickup_datetime timestamp
            , tpep_dropoff_datetime timestamp
            , passenger_count integer
            , trip_distance double precision
            , RatecodeID text
            , store_and_fwd_flag text
            , PULocationID text
            , DOLocationID text
            , payment_type integer
            , fare_amount double precision
            , extra double precision
            , mta_tax double precision
            , tip_amount double precision
            , tolls_amount double precision
            , improvement_surcharge double precision
            , total_amount double precision
            , congestion_surcharge double precision
        );
        CREATE TABLE IF NOT EXISTS {staging} (LIKE {table});
        TRUNCATE TABLE {staging};
        """
        pg_hook.run(schema_sql)

    @task(task_id="yellow_workflow.copy_and_merge")
    def yellow_copy_and_merge(file_path: str, meta: dict):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        staging = meta["staging_table"]
        table = meta["table"]
        filename = meta["file_name"]

        # Fast CopyIn using PostgreSQL COPY
        columns = (
            "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge"
        )
        copy_sql = f"COPY {staging} ({columns}) FROM STDIN WITH CSV HEADER"
        pg_hook.copy_expert(copy_sql, file_path)

        # Update unique_row_id & filename, then MERGE into target table
        transformation_sql = f"""
        UPDATE {staging}
        SET unique_row_id = md5(
                COALESCE(CAST(VendorID AS text), '') || COALESCE(CAST(tpep_pickup_datetime AS text), '') || 
                COALESCE(CAST(tpep_dropoff_datetime AS text), '') || COALESCE(PULocationID, '') || 
                COALESCE(DOLocationID, '') || COALESCE(CAST(fare_amount AS text), '') || 
                COALESCE(CAST(trip_distance AS text), '')
            ),
            filename = '{filename}';

        MERGE INTO {table} AS T
        USING {staging} AS S ON T.unique_row_id = S.unique_row_id
        WHEN NOT MATCHED THEN
            INSERT (unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
                    DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                    improvement_surcharge, total_amount, congestion_surcharge)
            VALUES (S.unique_row_id, S.filename, S.VendorID, S.tpep_pickup_datetime, S.tpep_dropoff_datetime,
                    S.passenger_count, S.trip_distance, S.RatecodeID, S.store_and_fwd_flag, S.PULocationID,
                    S.DOLocationID, S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount,
                    S.improvement_surcharge, S.total_amount, S.congestion_surcharge);
        """
        pg_hook.run(transformation_sql)

    # --- GREEN TAXI PIPELINE ---
    @task(task_id="green_workflow.create_tables")
    def green_create_tables(meta: dict):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        table = meta["table"]
        staging = meta["staging_table"]
        
        schema_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            unique_row_id text
            , filename text
            , VendorID text
            , lpep_pickup_datetime timestamp
            , lpep_dropoff_datetime timestamp
            , store_and_fwd_flag text
            , RatecodeID text
            , PULocationID text
            , DOLocationID text
            , passenger_count integer
            , trip_distance double precision
            , fare_amount double precision
            , extra double precision
            , mta_tax double precision
            , tip_amount double precision
            , tolls_amount double precision
            , ehail_fee double precision
            , improvement_surcharge double precision
            , total_amount double precision
            , payment_type integer
            , trip_type integer
            , congestion_surcharge double precision
        );
        CREATE TABLE IF NOT EXISTS {staging} (LIKE {table});
        TRUNCATE TABLE {staging};
        """
        pg_hook.run(schema_sql)

    @task(task_id="green_workflow.copy_and_merge")
    def green_copy_and_merge(file_path: str, meta: dict):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        staging = meta["staging_table"]
        table = meta["table"]
        filename = meta["file_name"]

        columns = (
            "VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,trip_type,congestion_surcharge"
        )
        copy_sql = f"COPY {staging} ({columns}) FROM STDIN WITH CSV HEADER"
        pg_hook.copy_expert(copy_sql, file_path)

        transformation_sql = f"""
        UPDATE {staging}
        SET unique_row_id = md5(
                COALESCE(CAST(VendorID AS text), '') || COALESCE(CAST(lpep_pickup_datetime AS text), '') || 
                COALESCE(CAST(lpep_dropoff_datetime AS text), '') || COALESCE(PULocationID, '') || 
                COALESCE(DOLocationID, '') || COALESCE(CAST(fare_amount AS text), '') || 
                COALESCE(CAST(trip_distance AS text), '')
            ),
            filename = '{filename}';

        MERGE INTO {table} AS T
        USING {staging} AS S ON T.unique_row_id = S.unique_row_id
        WHEN NOT MATCHED THEN
            INSERT (unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime,
                    store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count,
                    trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee,
                    improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge)
            VALUES (S.unique_row_id, S.filename, S.VendorID, S.lpep_pickup_datetime, S.lpep_dropoff_datetime,
                    S.store_and_fwd_flag, S.RatecodeID, S.PULocationID, S.DOLocationID, S.passenger_count,
                    S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee,
                    S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge);
        """
        pg_hook.run(transformation_sql)

    @task(trigger_rule="none_failed_min_one_success")
    def purge_files(file_path: str):
        """Removes extracted temporary CSV file (Equivalent to PurgeCurrentExecutionFiles)."""
        if os.path.exists(file_path):
            os.remove(file_path)

    # Pipeline Wiring
    metadata = set_label()
    downloaded_file = extract(metadata)
    branch = branch_by_taxi_type(metadata)

    # Yellow branch workflow
    y_create = yellow_create_tables(metadata)
    y_merge = yellow_copy_and_merge(downloaded_file, metadata)
    
    # Green branch workflow
    g_create = green_create_tables(metadata)
    g_merge = green_copy_and_merge(downloaded_file, metadata)

    purge = purge_files(downloaded_file)

    # Dependencies
    branch >> [y_create, g_create]
    y_create >> y_merge >> purge
    g_create >> g_merge >> purge

postgres_taxi_pipeline()