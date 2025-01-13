"""
Get historical weather data for a specific city and date.

EXERCISES:
1. Set the maximum number of consecutive failed DAG runs to 10.
2. Turn the get_max_wind task into a producer for the Dataset("wind_speed_data").
3. Turn the get_wind_direction task into a producer for the Dataset("wind_direction_data").
"""

from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.models.baseoperator import chain_linear
from airflow.models.param import Param
from airflow.datasets import Dataset
from pendulum import datetime, duration
import logging

t_log = logging.getLogger("airflow.task")

_MAX_TEMP_TASK_ID = "get_max_temp"
_WIND_SPEED_TASK_ID = "get_wind_speed"
_WIND_DIRECTION_TASK_ID = "get_wind_direction"
_WILDCARD_TASK_ID = "get_wildcard_data"


@dag(
    dag_display_name="1. Exercise upstream DAG 2 🌦️",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(minutes=1),
        "retry_exponential_backoff": True,
    },
    params={
        "my_city": Param(
            "Bern",
            type="string",
            title="City of interest:",
            description="Enter the city you want to retrieve historic weather data for.",
        ),
        "my_date_of_birth": Param(
            "1994-10-18T14:00:00+00:00",
            type="string",
            format="date-time",
        ),
        "get_max_temp": Param(
            True,
            type="boolean",
            title="Get max temperature for my birthday",
        ),
        "get_max_wind_speed": Param(
            True,
            type="boolean",
            title="Get max wind speed for my birthday",
        ),
        "get_wind_direction": Param(
            True,
            type="boolean",
            title="Get the dominant wind direction for my birthday",
            description="Wind direction is returned in degrees from 0 to 360. 0 is wind coming from the North, 90 from the East, 180 from the South, and 270 from the West.",
        ),
        "get_wildcard_data": Param(
            True,
            type="boolean",
            title="Get data from the 'wildcard_conn' connection",
        ),
    },
    tags=["exercise", "exercise_1", "exercise_2"],
)
def upstream_dag_2():

    @task
    def get_lat_long_for_one_city(**context) -> dict:
        """Converts a string of a city name provided into
        lat/long coordinates."""
        import requests

        city = context["params"]["my_city"]

        r = requests.get(f"https://photon.komoot.io/api/?q={city}")
        long = r.json()["features"][0]["geometry"]["coordinates"][0]
        lat = r.json()["features"][0]["geometry"]["coordinates"][1]

        t_log.info(f"Coordinates for {city}: {lat}/{long}")

        return {"city": city, "lat": lat, "long": long}

    city_coordinates = get_lat_long_for_one_city()

    @task
    def reformat_date(**context) -> str:
        from datetime import datetime

        date_of_birth = context["params"]["my_date_of_birth"]
        date_of_birth = datetime.fromisoformat(date_of_birth).strftime("%Y-%m-%d")

        return date_of_birth

    reformatted_date = reformat_date()

    @task.branch
    def determine_data_to_get(**context):
        task_ids_to_run = []

        if context["params"]["get_max_temp"]:
            task_ids_to_run.append(_MAX_TEMP_TASK_ID)
        if context["params"]["get_max_wind_speed"]:
            task_ids_to_run.append(_WIND_SPEED_TASK_ID)
        if context["params"]["get_wind_direction"]:
            task_ids_to_run.append(_WIND_DIRECTION_TASK_ID)
        if context["params"]["get_wildcard_data"]:
            task_ids_to_run.append(_WILDCARD_TASK_ID)

        return task_ids_to_run

    get_max_temp = HttpOperator(
        task_id=_MAX_TEMP_TASK_ID,
        endpoint="archive",
        method="GET",
        http_conn_id="historical_weather_api_conn",
        log_response=True,
        data={
            "latitude": city_coordinates["lat"],
            "longitude": city_coordinates["long"],
            "start_date": reformatted_date,
            "end_date": reformatted_date,
            "daily": "temperature_2m_max",
            "timezone": "auto",
        },
        outlets=[Dataset("max_temp_data")],
    )

    get_max_wind = HttpOperator(
        task_id=_WIND_SPEED_TASK_ID,
        endpoint="archive",
        method="GET",
        http_conn_id="historical_weather_api_conn",
        log_response=True,
        data={
            "latitude": city_coordinates["lat"],
            "longitude": city_coordinates["long"],
            "start_date": reformatted_date,
            "end_date": reformatted_date,
            "daily": "wind_speed_10m_max",
            "timezone": "auto",
        },
        ## EXERCISE 1: Turn this task into a producer for the Dataset("wind_speed_data")
        ## START CODE HERE ##
        ## END CODE HERE ##
    )

    get_wind_direction = HttpOperator(
        task_id=_WIND_DIRECTION_TASK_ID,
        endpoint="archive",
        method="GET",
        http_conn_id="historical_weather_api_conn",
        log_response=True,
        data={
            "latitude": city_coordinates["lat"],
            "longitude": city_coordinates["long"],
            "start_date": reformatted_date,
            "end_date": reformatted_date,
            "daily": "wind_direction_10m_dominant",
            "timezone": "auto",
        },
        ## EXERCISE 1: Turn this task into a producer for the Dataset("wind_direction_data")
        ## START CODE HERE ##
        ## END CODE HERE ##
    )

    get_wildcard = HttpOperator(
        task_id=_WILDCARD_TASK_ID,
        endpoint="",
        method="GET",
        http_conn_id="wildcard_conn",
        log_response=True,
        outlets=[Dataset("wildcard_data")],
    )

    chain_linear(
        [determine_data_to_get(), city_coordinates, reformatted_date],
        [get_max_temp, get_max_wind, get_wind_direction, get_wildcard],
    )


upstream_dag_2()
