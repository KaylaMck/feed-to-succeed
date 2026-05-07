import os

import polars as pl
import requests
from dotenv import load_dotenv
from loguru import logger
from snowflake.connector import connect

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

    census_data = pl.DataFrame(rows, schema=headers, orient="row")

    logger.info(f"Successfully fetched {len(census_data)} rows from Census ACS API")

    return census_data

def load_to_snowflake(census: pl.DataFrame) -> None:
    logger.info("Connecting to Snowflake...")

    conn = connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

    cursor = conn.cursor()

    logger.info("Creating table if not exists...")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.census_acs (
            B17001_001E STRING,
            B17001_002E STRING,
            B01003_001E STRING,
            B03002_003E STRING,
            B03002_004E STRING,
            B03002_012E STRING,
            state STRING
        )
    """)

    logger.info("Truncating existing data in raw.census_acs...")

    cursor.execute("TRUNCATE TABLE raw.census_acs")

    logger.info("Inserting new data into raw.census_acs...")
    
    rows = census.rows()
    cursor.executemany(
        "INSERT INTO raw.census_acs VALUES (%s, %s, %s, %s, %s, %s, %s)",
        rows
    )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Successfully loaded {len(census)} rows into Snowflake")

if __name__ == "__main__":
    census_data = fetch_census_data()
    load_to_snowflake(census_data)
