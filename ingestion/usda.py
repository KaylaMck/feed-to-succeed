import io
import os

import boto3
import polars as pl
from dotenv import load_dotenv
from loguru import logger
from snowflake.connector import connect

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
NSLP_KEY = os.getenv("S3_NSLP_KEY")
SBP_KEY = os.getenv("S3_SBP_KEY")

def fetch_usda_data(s3_key: str, program_name: str) -> pl.DataFrame:
    logger.info(f"Fetching {program_name} data from S3...")

    session = boto3.Session(profile_name=os.getenv("AWS_PROFILE"))
    s3 = session.client("s3", region_name=os.getenv("AWS_REGION"))

    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    excel_bytes = io.BytesIO(response["Body"].read())

    program_data = pl.read_excel(excel_bytes)

    logger.info(f"Successfully fetched {len(program_data)} rows for {program_name} from S3")

    return program_data

def load_to_snowflake(program_data: pl.DataFrame, table_name: str) -> None:
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

    columns = ", ".join([f'"{col}" STRING' for col in program_data.columns])

    logger.info("Creating table if not exists...")

    cursor.execute(f"CREATE TABLE IF NOT EXISTS raw.{table_name} ({columns})")

    logger.info(f"Truncating existing data in raw.{table_name}...")

    cursor.execute(f"TRUNCATE TABLE raw.{table_name}")

    logger.info(f"Loading {len(program_data)} row into Snowflake...")

    placeholders = ", ".join(["%s"] * len(program_data.columns))
    rows = program_data.rows()

    cursor.executemany(
        f'INSERT INTO raw.{table_name} VALUES ({placeholders})',
        rows
    )

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Successfully loaded {len(program_data)} rows to raw.{table_name}")

if __name__ == "__main__":
    nslp_data = fetch_usda_data(NSLP_KEY, "NSLP")
    load_to_snowflake(nslp_data, "nslp_participation")

    sbp_data = fetch_usda_data(SBP_KEY, "SBP")
    load_to_snowflake(sbp_data, "sbp_participation")
