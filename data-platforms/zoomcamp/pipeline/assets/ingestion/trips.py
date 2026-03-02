"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

materialization:
  type: table
  strategy: append

@bruin"""

import json
import os
from datetime import datetime, timezone

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _months_in_range(start_date: str, end_date: str):
    """Yield (year, month) tuples for every month in [start_date, end_date)."""
    current = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(day=1)
    while current < end:
        yield current.year, current.month
        current += relativedelta(months=1)


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]

    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    extracted_at = datetime.now(timezone.utc).isoformat()

    frames = []
    for taxi_type in taxi_types:
        for year, month in _months_in_range(start_date, end_date):
            filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            url = BASE_URL + filename
            print(f"Fetching {url}")
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            df = pd.read_parquet(pd.io.common.BytesIO(response.content))
            df["taxi_type"] = taxi_type
            df["extracted_at"] = extracted_at
            frames.append(df)

    return pd.concat(frames, ignore_index=True)
