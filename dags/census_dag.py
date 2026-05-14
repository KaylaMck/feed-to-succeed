import pendulum
from airflow.sdk import dag, task

@task()
def ingest_census():
    from ingestion.census import main

    main()

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["census", "demographics"],
)

def census_dag():
    ingest_census()

census_dag()