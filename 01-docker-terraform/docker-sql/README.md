## Run PGSQL in a Docker Container

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v ny_taxi_postgres_data:/var/lib/postgresql \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18

## Run another container in the same network
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -v pgadmin_data:/var/lib/pgadmin \
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4

## Connect to data using pgcli

uv run pgcli -h localhost -p 5432 -u root -d ny_taxi

## Run the script

uv run python ingest_data.py   --pg-user=root   --pg-pass=root   --pg-host=localhost   --pg-port=5432   --month=3 --pg-db=ny_taxi  --month=2  --target-table=yellow_taxi_trips_2021_2

## Run dockerized ingestion
docker run -it \
  --network=pipeline_pg-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips