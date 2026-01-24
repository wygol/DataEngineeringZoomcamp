import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

# this script doesnt have a good design because it is redundant at some places
# will refactor it in the future (TODO)
# to fix redundancy, pull the common stuff into the click.group (cli)

# using a click group to build a cli tool that can differentiate downloading data from the 
# DTC github repo and from downloading data (csv or parquet) from a link
@click.group()
def cli():
    """Data Ingestion Tool for dockerized Postgres database\nBuild to learn data ingestion pipelines"""
    pass


@cli.command(name="github")
@click.option(
    "--year",
    default="2021",
    type=str,
    help="The year the gitHub dataset will be filtered after",
)
@click.option(
    "--month",
    default="01",
    type=str,
    help="The month the gitHub dataset will be filtered after",
)
@click.option("--user", default="root", help="PostgreSQL user")
@click.option("--password", default="root", type=str, help="PostgreSQL password")
@click.option("--host", default="localhost", type=str, help="PostgreSQL host")
@click.option("--port", default=5432, type=int, help="PostgreSQL port")
@click.option("--db", default="ny_taxi", type=str, help="PostgreSQL database name")
@click.option("--table", default="yellow_taxi_data", type=str, help="Target table name")
@click.option(
    "--schema",
    is_flag=True,
    type=bool,
    help="Want to see the schema printed that had been ingested?",
)
def ingest_data_cli(year:str, month:str, user:str, password:str, host:str, port:int, db:str, table:str, schema:bool):
    """Ingesting data from the DTC github repository (this data is already very clean and just for training)"""
    ingest_data(
        year=year,
        month=month,
        user=user,
        password=password,
        host=host,
        port=port,
        db=db,
        table=table,
        schema=schema,
    )


# Note: CLI is seperated from the logic, so that ingest_data could be tested in the future if it was more complex...

# writing another function to download from another source (with link)

@cli.command(name="link")
@click.option("--user", default="root", help="PostgreSQL user")
@click.option("--password", default="root", type=str, help="PostgreSQL password")
@click.option("--host", default="localhost", type=str, help="PostgreSQL host")
@click.option("--port", default=5432, type=int, help="PostgreSQL port")
@click.option("--db", default="ny_taxi", type=str, help="PostgreSQL database name")
@click.option("--table", default="yellow_taxi_data", type=str, help="Target table name")
@click.option(
    "--schema",
    is_flag=True,
    type=bool,
    help="Want to see the schema printed that had been ingested?",
)
@click.option("--url", default="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet", type=str, help="Full url to data source")
@click.option("--dtype", default="parquet", type=click.Choice(["parquet", "csv"], case_sensitive=False), help="choose if data source is parquet or csv")
def ingest_data_from_link_cli(user, password, host, port, db, table, schema, url, dtype):
    """Ingesting data from arbitrary URLs (I do not use any explicit checks, just pure pandas which could become problematic)
    Data will be pushed into the database"""
    ingest_data_from_link(user=user,
        password=password,
        host=host,
        port=port,
        db=db,
        table=table,
        schema=schema, url=url, dtype=dtype)



def ingest_data_from_link(user: str,
    password: str,
    host: str,
    port: int,
    db: str,
    table: str,
    schema: bool,url:str, dtype:str):
    url = url

    # declaring types of columns after inspection
    dtypes = {
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
        "congestion_surcharge": "float64",
    }

    parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    # building connection path to database
    engine = create_engine(
        "postgresql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, db)
    )

    # reading data in chunks in proper defined way
    
    if dtype == "parquet":
        df_iter = pd.read_parquet(url)
        # for parquet there exists no iterator. I will also have to check whether the data fits the db schema...

        df_iter.head(0).to_sql(name=table, con=engine, if_exists="replace")
        print("Table created")

        df_iter.to_sql(name=table, con=engine, if_exists="append")
        print("Data inserted successfully")
        # this is a pretty bad way because the database will be locked for minutes...

        if schema:
            print(pd.io.sql.get_schema(df_iter.head(0), name=table, con=engine))

        
    elif dtype =="csv":
        df_iter = pd.read_csv(
            url, dtype=dtypes, parse_dates=parse_dates, iterator=True, chunksize=100000
        )

        # add each chunk individually to the db now
        first = True
        for chunk in tqdm(df_iter):
            if first:
                # first push the fresh schema and the data afterwards
                chunk.head(0).to_sql(name=table, con=engine, if_exists="replace")
    
                first = False
                print("Table created")
    
            chunk.to_sql(
                name=table, con=engine, if_exists="append", chunksize=2000, method="multi"
            )
            print("inserted: {0}".format(len(chunk)))
    
        if schema:
            # schema look
            print(pd.io.sql.get_schema(chunk.head(0), name=table, con=engine))

    return


def ingest_data(
    year: str,
    month: str,
    user: str,
    password: str,
    host: str,
    port: int,
    db: str,
    table: str,
    schema: bool,
) -> None:
    # Ingestion logic here

    # read data from the github repo (building link)
    # link can later be changed or become a parameter so that
    # multiple files can be downloaded
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/"
    filename = "yellow_tripdata_{0}-{1}.csv.gz".format(year, month)
    url = prefix + filename

    # declaring types of columns after inspection
    dtypes = {
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
        "congestion_surcharge": "float64",
    }

    parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    # building connection path to database
    engine = create_engine(
        "postgresql://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, db)
    )

    # reading data in chunks in proper defined way
    df_iter = pd.read_csv(
        url, dtype=dtypes, parse_dates=parse_dates, iterator=True, chunksize=100000
    )

    # add each chunk individually to the db now
    first = True
    for chunk in tqdm(df_iter):
        if first:
            # first push the fresh schema and the data afterwards
            chunk.head(0).to_sql(name=table, con=engine, if_exists="replace")

            first = False
            print("Table created")

        chunk.to_sql(
            name=table, con=engine, if_exists="append", chunksize=2000, method="multi"
        )
        print("inserted: {0}".format(len(chunk)))

    if schema:
        # schema look
        print(pd.io.sql.get_schema(chunk.head(0), name=table, con=engine))

    return


if __name__ == "__main__":
    cli()