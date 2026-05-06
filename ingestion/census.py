import os

import polars as pl
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

API_KEY = os.getenv("CENSUS_API_KEY")
BASE_URL = "https://api.census.gov/data/2023/acs/acs5"

VARIABLES = {
    "B17001_001E": "poverty_population_total",
    "B17001_002E": "poverty_population_below",
    "B01003_001E": "total_population",
    "B03002_003E": "white_alone",
    "B03002_004E": "black_alone",
    "B03002_012E": "hispanic_latino",
}


def fetch_census_data():
    logger.info("Starting Census ACS Data Fetch...")

    variables = ",".join(VARIABLES.keys())

    params = {
        "get": variables,
        "for": "state:*",
        "key": API_KEY,
    }

    logger.info(f"Fetching variables: {list(VARIABLES.keys())}")

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()

    headers = data[0]
    rows = data[1:]

    census = pl.DataFrame(rows, schema=headers, orient="row")

    logger.info(f"Successfully fetched {len(census)} rows from Census ACS API")

    return census

if __name__ == "__main__":
    census_data = fetch_census_data()
    print(census_data.head())