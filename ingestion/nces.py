import os

import polars as pl
import requests
from dotenv import load_dotenv
from loguru import logger
from snowflake.connector import connect

load_dotenv()

BASE_URL = "https://www.nationsreportcard.gov/Dataservice/GetAdhocData.aspx"

ALL_STATES = (
    "AL,AK,AZ,AR,CA,CO,CT,DE,FL,GA,HI,ID,IL,IN,IA,KS,KY,LA,ME,MD,"
    "MA,MI,MN,MS,MO,MT,NE,NV,NH,NJ,NM,NY,NC,ND,OH,OK,OR,PA,RI,SC,"
    "SD,TN,TX,UT,VT,VA,WA,WV,WI,WY,DC"
)

MATH_PARAMS = {
    "type": "data",
    "subject": "mathematics",
    "grade": "8",
    "subscale": "MRPCM",
    "variable": "TOTAL",
    "jurisdiction": ALL_STATES,
    "stattype": "MN:MN",
    "Year": "2019,2022,2024",
}

READING_PARAMS = {
    "type": "data",
    "subject": "reading",
    "grade": "8",
    "subscale": "RRPCM",
    "variable": "TOTAL",
    "jurisdiction": ALL_STATES,
    "stattype": "MN:MN",
    "Year": "2019,2022,2024",
}

def fetch_nces_data(params: dict, subject_name: str) -> pl.DataFrame:
    logger.info(f"Fetching NCES {subject_name} data...")

    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json()
    records = data.get("result", [])

    nces_data = pl.DataFrame(records)

    logger.info(f"Successfully fetched {len(nces_data)} rows for {subject_name}")

    return nces_data

def load_to_snowflake(nces_data: pl.DataFrame, table_name: str) -> None:
    logger.info(f"Connecting to Snowflake...")

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

    columns = ", ".join([f'"{col}" STRING' for col in nces_data.columns])

    logger.info("Creating table if not exists...")
    cursor.execute(f"CREATE TABLE IF NOT EXISTS raw.{table_name} ({columns})")

    logger.info(f"Truncating existing data in raw.{table_name}...")
    cursor.execute(f"TRUNCATE TABLE raw.{table_name}")

    placeholders = ", ".join(["%s"] * len(nces_data.columns))
    rows = nces_data.rows()

    logger.info(f"Loading {len(nces_data)} rows into Snowflake...")
    cursor.executemany(
        f"INSERT INTO raw.{table_name} VALUES ({placeholders})",
        rows
    )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Successfully loaded {len(nces_data)} rows to raw.{table_name}")

if __name__ == "__main__":
    math_data = fetch_nces_data(MATH_PARAMS, "mathematics")
    print(math_data.head())
    load_to_snowflake(math_data, "nces_math")

    reading_data = fetch_nces_data(READING_PARAMS, "reading")
    print(reading_data.head())
    load_to_snowflake(reading_data, "nces_reading")
