from pathlib import Path

import pendulum
from airflow.sdk import dag
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="nutrition_pipeline",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": "feed_to_succeed",
            "schema": "marts",
            "warehouse": "feed_to_succeed_wh",
            "role": "ACCOUNTADMIN",
        },
    ),
)

@dag(
    schedule="@monthly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "marts", "nutrition_pipeline"]
)

def dbt_marts_dag():
    DbtTaskGroup(
        group_id="dbt_marts",
        project_config=ProjectConfig(
            dbt_project_path=Path("/opt/airflow/nutrition_pipeline"),
        ),
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["marts"]
        ),
    )

dbt_marts_dag()
