import pendulum
from airflow.sdk import dag, task

@task()
def ingest_usda():
    from ingestion.usda import main

    main()

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["usda", "nutrition"],
)

def usda_dag():
    ingest_usda()

usda_dag()