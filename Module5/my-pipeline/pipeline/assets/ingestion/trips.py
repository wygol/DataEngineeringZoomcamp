"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

# columns:
#   - name: TODO_col1
#     type: TODO_type
#     description: TODO

# will use the columns in a real project for metadata, documentation, and testing.
# For this demo, I skip defining them upfront since the focus is on architecture and Bruin concepts.

@bruin"""

def materialize():
    """
    Fetch NYC taxi trip data from TLC public endpoint.

    Uses Bruin runtime context:
    - BRUIN_START_DATE / BRUIN_END_DATE: Date range for data extraction
    - BRUIN_VARS: Pipeline variables including taxi_types

    Returns a DataFrame with raw taxi trip data plus extraction metadata.
    Duplicates are handled in staging layer.
    """
    import os
    import json
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    import pandas as pd

    # Get date range from Bruin environment
    start_date_str = os.getenv("BRUIN_START_DATE")
    end_date_str = os.getenv("BRUIN_END_DATE")
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    
    # Get taxi types from pipeline variables
    bruin_vars = os.getenv("BRUIN_VARS", "{}")
    vars_dict = json.loads(bruin_vars)
    taxi_types = vars_dict.get("taxi_types", ["yellow"])
    
    # TLC endpoint base URL
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    # Generate list of files to fetch based on date range and taxi types
    files_to_fetch = []
    current_date = start_date
    
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        
        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            url = f"{base_url}/{filename}"
            files_to_fetch.append(url)
        
        # Move to next month
        current_date += relativedelta(months=1)
    
    # Fetch and concatenate data
    dfs = []
    for url in files_to_fetch:
        try:
            df = pd.read_parquet(url)
            dfs.append(df)
            print(f"✓ Fetched: {url.split('/')[-1]} ({len(df)} rows)")
        except Exception as e:
            print(f"✗ Failed to fetch {url}: {e}")
    
    if not dfs:
        raise ValueError(f"No data fetched for the date range {start_date} to {end_date}")
    
    # Combine all dataframes
    result_df = pd.concat(dfs, ignore_index=True)
    
    # Add extraction metadata for lineage and debugging
    result_df["extracted_at"] = datetime.utcnow().isoformat()
    
    return result_df


