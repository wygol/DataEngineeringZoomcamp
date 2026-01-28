import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import yaml
from typing import NamedTuple

class SchemaConfig(NamedTuple):
    dtypes : dict
    parse_dates : list

# loading schema definition for csv
def load_schema(file_path:str, table_name:str) -> SchemaConfig:
    with open(file_path, "r") as file:
        config = yaml.safe_load(file)
        
        
    table_config = config.get(table_name)
    # this assumes that date columns exist...
    if table_config is None:
        print("Warning! Empty Schema has been used. Deletion of table is recommended")
        return SchemaConfig(dtypes={}, parse_dates=[])
    else:
        return SchemaConfig(dtypes=table_config.get("columns", {}), parse_dates=table_config.get("date_columns", []))




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
@click.option("--table", default="yellow_taxi_data", type=str, help="Target table name (schema file will also be checked against this table name)")
@click.option(
    "--schema",
    is_flag=True,
    type=bool,
    help="Want to see the schema printed that had been ingested?",
)
@click.option("--url", required=True,
              type=str, help="Full url to data source")
@click.option("--dtype", default="parquet", type=click.Choice(["parquet", "csv"], case_sensitive=False),
              help="choose if data source is parquet or csv")
@click.option("--schemafile", type=click.Path(exists=True), help="Path to YAML schema file (only important for csv)")
def ingest_data_from_link_cli(user, password, host, port, db, table, schema, url, dtype, schemafile):
    """Ingesting data from arbitrary URLs (I do not use any explicit checks, just pure pandas which could become problematic)
    Data will be pushed into the database"""
    
    
    dtypes, parse_dates = {}, []
    if dtype == "csv":
        if not schemafile:
            raise click.UsageError("--schemafile is required when using --dtype=csv")
        schemaConfig = load_schema(file_path=schemafile, table_name=table)
        dtypes = schemaConfig.dtypes
        parse_dates = schemaConfig.parse_dates
        click.echo("Loaded schema for {0} from {1} successfully".format(table, schemafile))
    
    
    ingest_data_from_link(user=user,
        password=password,
        host=host,
        port=port,
        db=db,
        table=table,
        schema=schema, url=url, dtype=dtype, dtypes=dtypes, parse_dates=parse_dates)



def ingest_data_from_link(user: str,
    password: str,
    host: str,
    port: int,
    db: str,
    table: str,
    schema: bool,url:str, dtype:str, dtypes:dict, parse_dates:list):
    url = url

    # declaring types of columns after inspection
    dtypes = dtypes

    parse_dates = parse_dates
    
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
    # I will also have to change this to use the yaml file because the github data is also always different
    # but when I do this I can refactor the entire code in the same instance, because right now this is a mess. TODO
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