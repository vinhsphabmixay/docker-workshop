import json
import logging
import tempfile
import os
from datetime import datetime
import requests
import duckdb

from airflow.decorators import dag, task
from airflow.models.param import Param

@dag(
    dag_id="03_getting_started_data_pipeline",
    description="Fetches product data, transforms fields with Python, and queries averages using DuckDB",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "columns_to_keep": Param(
            ["brand", "price"],
            type="array",
            description="List of product attributes to retain during transformation",
        )
    },
    tags=["zoomcamp"],
)
def data_pipeline():

    @task
    def extract() -> dict:
        """Downloads product data via HTTP request."""
        url = "https://dummyjson.com/products"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task
    def transform(data: dict, **context) -> list[dict]:
        """Filters input JSON based on configured DAG parameters."""
        columns_to_keep = context["params"]["columns_to_keep"]

        filtered_data = [
            {column: product.get(column, "N/A") for column in columns_to_keep}
            for product in data.get("products", [])
        ]
        return filtered_data

    @task
    def query(filtered_data: list[dict]) -> list[tuple]:
        """Executes analytical DuckDB query directly on transformed data structure."""
        conn = duckdb.connect(database=":memory:")
        
        # Write JSON to temporary file for DuckDB to read
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(filtered_data, f)
            temp_file = f.name
        
        try:
            query_sql = f"""
                SELECT brand, round(avg(price), 2) as avg_price
                FROM read_json_auto('{temp_file}')
                GROUP BY brand
                ORDER BY avg_price DESC;
            """
            results = conn.execute(query_sql).fetchall()
            
            logging.info("DuckDB Aggregation Results:")
            for row in results:
                logging.info(f"Brand: {row[0]} | Avg Price: {row[1]}")

            return results
        finally:
            # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)

    # Dataflow Pipeline Dependencies
    extracted_raw = extract()
    transformed_data = transform(extracted_raw)
    query(transformed_data)

data_pipeline()
