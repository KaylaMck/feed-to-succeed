import pendulum
from airflow.sdk import dag, task

@task()
def ingest_nces():
    from ingestion.nces import main

    main()

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["nces", "education"],
)

def nces_dag():
    ingest_nces()

nces_dag()