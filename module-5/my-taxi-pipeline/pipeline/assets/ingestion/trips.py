"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate list of months between start and end dates
    months = []
    current = start.replace(day=1)
    while current < end:
        months.append(current)
        current += relativedelta(months=1)

    all_dfs = []
    for taxi_type in taxi_types:
        for month in months:
            year = month.strftime("%Y")
            month_str = month.strftime("%m")
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"
            try:
                df = pd.read_parquet(url)
                pickup_col = [c for c in df.columns if "pickup" in c.lower() and "datetime" in c.lower()]
                dropoff_col = [c for c in df.columns if "dropoff" in c.lower() and "datetime" in c.lower()]

                df = df.rename(columns={
                    pickup_col[0]: "pickup_datetime",
                    dropoff_col[0]: "dropoff_datetime",
                })

                if "PULocationID" in df.columns:
                    df = df.rename(columns={"PULocationID": "pickup_location_id"})
                if "DOLocationID" in df.columns:
                    df = df.rename(columns={"DOLocationID": "dropoff_location_id"})
                if "fare_amount" not in df.columns:
                    df["fare_amount"] = 0.0
                if "payment_type" not in df.columns:
                    df["payment_type"] = 0

                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.now()

                df = df[["pickup_datetime", "dropoff_datetime", "pickup_location_id",
                         "dropoff_location_id", "fare_amount", "payment_type",
                         "taxi_type", "extracted_at"]]

                all_dfs.append(df)
                print(f"Fetched {len(df)} rows for {taxi_type} {year}-{month_str}")
            except Exception as e:
                print(f"Skipping {taxi_type} {year}-{month_str}: {e}")

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    return pd.DataFrame()
