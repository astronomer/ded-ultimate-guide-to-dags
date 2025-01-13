"""
Generate a report with the weather forecast for the cities and historical weather data.

This DAG fetches the weather forecast for the cities and historical weather data from the upstream DAGs
via XCom and generates a report with the data.
"""

from airflow.decorators import dag, task
from airflow.models.dataset import Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime
import pandas as pd
import json
import logging

t_log = logging.getLogger("airflow.task")


@dag(
    dag_display_name="Solution Downstream DAG 📒",
    start_date=datetime(2024, 6, 1),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        datasets=(
            Dataset("current_weather_data")
            & Dataset("max_temp_data")
            & (Dataset("wind_speed_data") | Dataset("wind_direction_data"))
        ),
        # Runs every day at midnight UTC
        # AND whenever both "current_weather_data" and "max_temp_data" are updated
        # AS WELL AS ONE OF the datasets "wind_speed_data" OR "wind_direction_data".
    ),
    catchup=False,
    max_consecutive_failed_dag_runs=6,
    doc_md=__doc__,
    description="Generate a report with the weather forecast for the cities and historical weather data.",
    default_args={"owner": "Astro", "retries": 3},
    tags=["solution"],
)
def downstream_dag_solution():

    @task
    def fetch_cities_weather_table(**context) -> pd.DataFrame:

        df = context["ti"].xcom_pull(
            dag_id="upstream_dag_1_solution",
            task_ids="create_weather_table",
            include_prior_dates=True,
        )

        return df

    @task
    def fetch_max_temp_data(**context) -> dict:

        data = context["ti"].xcom_pull(
            dag_id="upstream_dag_2_solution",
            task_ids="get_max_temp",
            include_prior_dates=True,
        )

        return json.loads(data) if data is not None else None

    @task
    def fetch_wind_speed_data(**context) -> dict:

        data = context["ti"].xcom_pull(
            dag_id="upstream_dag_2_solution",
            task_ids="get_wind_speed",
            include_prior_dates=True,
        )

        return json.loads(data) if data is not None else None

    @task
    def fetch_wind_direction_data(**context) -> dict:

        data = context["ti"].xcom_pull(
            dag_id="upstream_dag_2_solution",
            task_ids="get_wind_direction",
            include_prior_dates=True,
        )

        return json.loads(data) if data is not None else None

    @task
    def fetch_wildcard_data(**context) -> dict:

        data = context["ti"].xcom_pull(
            dag_id="upstream_dag_2_solution",
            task_ids="get_wildcard_data",
            include_prior_dates=True,
        )

        return json.loads(data) if data is not None else None

    @task
    def fetch_city_dag_2(**context) -> dict:

        data = context["ti"].xcom_pull(
            dag_id="upstream_dag_2",
            task_ids="get_lat_long_for_one_city",
            include_prior_dates=True,
        )

        return data

    @task
    def generate_report(
        cities_weather: pd.DataFrame,
        max_temp: dict,
        wind_speed: dict,
        wind_direction: dict,
        wildcard: dict,
        city_coordinates: dict,
    ):
        from tabulate import tabulate

        if cities_weather is not None:
            t_log.info("Current Weather data:")
            t_log.info(
                tabulate(
                    cities_weather, headers="keys", tablefmt="grid", showindex=True
                ),
            )

        if max_temp:
            city = city_coordinates["city"]
            date = max_temp["daily"]["time"][0]

            t_log.info("--------------------------")
            t_log.info(f"Historical Weather data for {city} on {date}:")
            t_log.info(
                f"Max temperature data: {max_temp['daily']['temperature_2m_max'][0]}"
            )
        if wind_speed:
            t_log.info(
                f"Wind speed data: {wind_speed['daily']['wind_speed_10m_max'][0]}"
            )
        if wind_direction:
            t_log.info(
                f"Wind direction data: {wind_direction['daily']['wind_direction_10m_dominant'][0]}"
            )
        t_log.info("--------------------------")
        if wildcard:
            t_log.info(f"Wildcard data: {wildcard}")

    generate_report(
        fetch_cities_weather_table(),
        fetch_max_temp_data(),
        fetch_wind_speed_data(),
        fetch_wind_direction_data(),
        fetch_wildcard_data(),
        fetch_city_dag_2(),
    )


downstream_dag_solution()
