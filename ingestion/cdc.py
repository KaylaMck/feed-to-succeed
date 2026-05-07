import os

import polars as pl
import requests
from dotenv import load_dotenv
from loguru import logger
from snowflake.connector import connect

load_dotenv()

BASE_URL = "https://chronicdata.cdc.gov/resource/vba9-s8jp.json"

def fetch_cdc_data() -> pl.DataFrame:
    logger.info("Starting CDC Data Fetch...")

    all_records = []
    limit = 20000
    offset = 0

    while True:
        params = {
            "$limit": limit,
            "$offset": offset
        }

        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()

        batch = response.json()

        if not batch:
            break

        all_records.extend(batch)
        offset += limit
        logger.info(f"Fetched {len(all_records)} records so far...")

    cdc_data = pl.DataFrame(all_records)

    logger.info(f"Successfully fetched {len(cdc_data)} rows from CDC API")

    return cdc_data

def load_to_snowflake(cdc_data: pl.DataFrame) -> None:
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

    columns = ", ".join([f'"{col}" STRING' for col in cdc_data.columns])

    logger.info("Creating table if not exists...")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS raw.cdc_data (
            {columns}
        )
     """)

    logger.info("Truncating existing data in raw.cdc_data...")
    cursor.execute("TRUNCATE TABLE raw.cdc_data")

    placeholders = ", ".join(["%s"] * len(cdc_data.columns))

    cdc_data = cdc_data.with_columns([
        pl.col(col).cast(pl.Utf8) for col in cdc_data.columns
    ])

    rows = cdc_data.rows()

    logger.info(f"Inserting {len(cdc_data)} rows into Snowflake...")

    cursor.executemany(
        f"INSERT INTO raw.cdc_data VALUES ({placeholders})",
        rows
    )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Successfully loaded {len(cdc_data)} rows to raw.cdc_data")


if __name__ == "__main__":
    cdc_data = fetch_cdc_data()
    load_to_snowflake(cdc_data)
