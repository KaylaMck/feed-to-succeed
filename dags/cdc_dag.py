import pendulum
from airflow.sdk import dag, task

@task()
def ingest_cdc():
    from ingestion.cdc import main

    main()

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["cdc", "health"],
)

def cdc_dag():
    ingest_cdc()

cdc_dag()