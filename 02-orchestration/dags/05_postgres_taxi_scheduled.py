import gzip
import os
import requests
from datetime import datetime
from pandas import date_range

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "ny_taxi_postgres"

TAXI_CONFIGS = {
    "green": {
        "schedule": "0 9 1 * *",
        "columns" : (
            "VendorID,lpep_pickup_datetime,lpep_dropoff_datetime,store_and_fwd_flag,RatecodeID,"
            "PULocationID,DOLocationID,passenger_count,trip_distance,fare_amount,extra,mta_tax,"
            "tip_amount,tolls_amount,ehail_fee,improvement_surcharge,total_amount,payment_type,"
            "trip_type,congestion_surcharge"
        ),
        "schema_sql" : """
            CREATE TABLE IF NOT EXISTS {table} (
                unique_row_id text, filename text, VendorID text, lpep_pickup_datetime timestamp,
                lpep_dropoff_datetime timestamp, store_and_fwd_flag text, RatecodeID text,
                PULocationID text, DOLocationID text, passenger_count integer, trip_distance double precision,
                fare_amount double precision, extra double precision, mta_tax double precision,
                tip_amount double precision, tolls_amount double precision, ehail_fee double precision,
                improvement_surcharge double precision, total_amount double precision, payment_type integer,
                trip_type integer, congestion_surcharge double precision
            );
            CREATE TABLE IF NOT EXISTS {staging} (LIKE {table});
            TRUNCATE TABLE {staging};
        """,
        "transform_n_merge_sql" : """
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
    },
    "yellow": {
        "schedule": "0 10 1 * *",
        "columns" : (
            "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,"
            "RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,"
            "extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge"
        ),
        "schema_sql" : """
            CREATE TABLE IF NOT EXISTS {table} (
                unique_row_id text, filename text, VendorID text, tpep_pickup_datetime timestamp,
                tpep_dropoff_datetime timestamp, passenger_count integer, trip_distance double precision,
                RatecodeID text, store_and_fwd_flag text, PULocationID text, DOLocationID text,
                payment_type integer, fare_amount double precision, extra double precision,
                mta_tax double precision, tip_amount double precision, tolls_amount double precision,
                improvement_surcharge double precision, total_amount double precision, congestion_surcharge double precision
            );
            CREATE TABLE IF NOT EXISTS {staging} (LIKE {table});
            TRUNCATE TABLE {staging};
        """,
        "transform_n_merge_sql" : """
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
    }
}

def create_taxi_dag(taxi_type: str, config: dict):
    dag_id = f"05_postgres_{taxi_type}_scheduled"

    @dag(
        dag_id=dag_id,
        description="Scheduled/Backfill workflow for NYC {taxi_type.capitalize()} data extraction and loading into Postgres",
        schedule=config["schedule"],
        start_date=datetime(2019, 1, 1),
        catchup=True,
        max_active_runs=1,  # Corresponds to Kestra concurrency limit: 1
        params={
            "start_year_month": Param(
                default="",
                type=["null","string"],
                description="Optional start month for backfill runs. Leave empty for scheduled/logical date"
            ),
            "end_year_month": Param(
                default="",
                type=["null","string"],
                description="Optional end month for backfill runs. Leave empty for scheduled/logical date"
            ),

        },
        tags=["zoomcamp", taxi_type]
    )

    def postgres_taxi_scheduled_pipeline():

        @task
        def generate_file_list(**context) -> list[dict]:
            # Airflow context logical_date handles scheduled runs and backfills
            start_ym = context["params"].get("start_year_month")
            end_ym = context["params"].get("end_year_month")

            if start_ym and start_ym.strip():
                start_str = start_ym.strip()
                end_str = end_ym.strip() if end_ym and end_ym.strip() else start_str

                dates = date_range(start=f"{start_str}-01", end=f"{end_str}-01", freq="MS")
                ym_list = [d.strftime("%Y-%m") for d in dates]
            else:
                logical_date = context["logical_date"]
                year_month = logical_date.strftime("%Y-%m")

            file_targets = []
            for ym in ym_list:
                file_name = f"{taxi_type}_tripdata_{ym}.csv"
                file_targets.append({
                    "year_month": ym,
                    "file_name": file_name,
                    "staging_table": f"public.{taxi_type}_tripdata_staging",
                    "table": f"public.{taxi_type}_tripdata",
                    "url": f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{file_name}.gz"
                })
            
            return file_targets

        @task
        def process_range(targets: list[dict]):
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

            for meta in targets:
                filename = meta["file_name"]
                url = meta["url"]
                staging_table = meta["staging_table"]
                table= meta["table"]
                local_filepath = os.path.join("/tmp", f"{filename}.gz")

                schema_sql = config["schema_sql"].format(table=table, staging=staging_table)
                pg_hook.run(schema_sql)

                response = requests.get(url, stream=True)
                if response.status_code != 200:
                    print(f"Failed to download file from {url}. Status code: {response.status_code}")
                    continue
                with gzip.GzipFile(fileobj=response.raw) as gz, open(local_filepath, "wb") as out_file:
                    out_file.write(gz.read())
                    
                copy_sql = f"COPY {staging_table} ({config['columns']}) FROM STDIN WITH CSV HEADER"
                pg_hook.copy_expert(copy_sql, local_filepath)

                merge_sql = config["transform_n_merge_sql"].format(table=table, staging=staging_table, filename=filename)
                pg_hook.run(merge_sql)

                if os.path.exists(local_filepath):
                    os.remove(local_filepath)
        
        # Dependencies Wiring
        targets = generate_file_list()
        process_range(targets)

    return postgres_taxi_scheduled_pipeline()

for taxi, cfg in TAXI_CONFIGS.items():
    globals()[f"05_postgres_{taxi}_scheduled"] = create_taxi_dag(taxi, cfg)