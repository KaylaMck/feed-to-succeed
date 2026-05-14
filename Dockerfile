FROM apache/airflow:latest

RUN pip install --no-cache-dir \
    snowflake-connector-python \
    dbt-snowflake \
    astronomer-cosmos \
    requests \
    polars \
    fastexcel \
    loguru \
    python-dotenv \
    boto3