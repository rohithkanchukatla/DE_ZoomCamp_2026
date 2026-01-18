import pandas as pd
import click
from sqlalchemy import create_engine
import os

# Using a variable for the data source URL to keep the code clean
DATA_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

@click.command()
@click.option("--user", envvar="DB_USER", prompt=True, help="PostgreSQL user")
@click.option("--password", envvar="DB_PASSWORD", prompt=True, hide_input=True, help="PostgreSQL password")
@click.option("--host", envvar="DB_HOST", prompt=True, help="PostgreSQL host")
@click.option("--port", envvar="DB_PORT", default=5432, type=int, help="PostgreSQL port")
@click.option("--db", envvar="DB_NAME", prompt=True, help="PostgreSQL database name")
@click.option("--table", envvar="TABLE_NAME", default="yellow_taxi_data", help="Target table name")
def ingest_data(user, password, host, port, db, table):
    # 1. Create the connection engine
    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(connection_url)

    # 2. Define schema types for optimized loading
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "RatecodeID": "Int64",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "store_and_fwd_flag": "string",
        "trip_distance": "float64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }

    parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    # 3. Read data in chunks to prevent memory overflow
    df_iter = pd.read_csv(
        DATA_URL,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000,
    )

    # 4. Ingestion loop
    first_chunk = True
    for df_chunk in df_iter:
        if first_chunk:
            # Create the table schema first
            df_chunk.head(0).to_sql(name=table, con=engine, if_exists="replace", index=False)
            first_chunk = False
            print(f"Table '{table}' created successfully.")

        # Append data
        df_chunk.to_sql(name=table, con=engine, if_exists="append", index=False, method="multi")
        print(f"Inserted chunk of {len(df_chunk)} rows.")

if __name__ == "__main__":
    ingest_data()