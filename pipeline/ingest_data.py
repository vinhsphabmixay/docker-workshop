import pandas as pd
from sqlalchemy import create_engine

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run():
    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = '5432'
    pg_database = 'ny_taxi'

    month, year = 1, 2021
    
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f"{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz"

    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_database}')

    target_table =  'yellow_taxi_data'

    chunksize = 100000


    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first = True

    for df_chunk in df_iter:

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))


if __name__ == '__main__':
    run()

