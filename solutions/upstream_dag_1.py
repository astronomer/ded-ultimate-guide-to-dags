"""
Retrieve weather information for a list of cities.

This DAG retrieves the latitude and longitude coordinates for each city provided 
in a DAG param and uses these coordinates to get weather information from 
a weather API. 
"""

from airflow.decorators import dag, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from airflow.datasets import Dataset
from pendulum import datetime, duration
import logging

t_log = logging.getLogger("airflow.task")


@dag(
    dag_display_name="Solution upstream DAG 1 🌤️",
    start_date=datetime(2024, 6, 1),
    schedule=None,
    max_consecutive_failed_dag_runs=10,
    catchup=False,
    doc_md=__doc__,
    description="Retrieve weather information for a list of cities.",
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": duration(minutes=1),
        "retry_exponential_backoff": True,
    },
    params={
        "my_cities": Param(
            ["Bern", "Zurich", "Lausanne"],
            type="array",
            title="Cities of interest:",
            description="Enter the cities you want to retrieve weather info for. One city per line.",
        ),
        "weather_parameter": Param(
            "temperature_2m",
            type="string",
            enum=[
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "cloud_cover",
                "wind_speed_10m",
            ],
        ),
        "timeframe": Param(
            "today",
            type="string",
            enum=["today", "yesterday", "today_and_tomorrow"],
            title="Forecast vs. Past Data",
            description="Choose whether you want to retrieve weather data/forecasts for yesterday, today or tomorrow.",
        ),
        "simulate_api_failure": Param(
            False,
            type="boolean",
            title="Simulate API failure",
            description="Set to true to simulate an API failure in the get_lat_long_for_one_city task.",
        ),
        "simulate_task_delay": Param(
            0,
            type="number",
            title="Simulate task delay",
            description="Set the number of seconds to delay the create_weather_table task of this DAG.",
        ),
    },
    tags=["solution"],
)
def upstream_dag_1_solution():

    @task
    def get_cities(**context) -> list:
        return context["params"]["my_cities"]

    cities = get_cities()

    @task
    def get_lat_long_for_one_city(city: str, **context) -> dict:
        """Converts a string of a city name provided into
        lat/long coordinates."""
        import requests

        if context["params"]["simulate_api_failure"]:
            raise Exception("Simulated API failure.")

        r = requests.get(f"https://photon.komoot.io/api/?q={city}")
        long = r.json()["features"][0]["geometry"]["coordinates"][0]
        lat = r.json()["features"][0]["geometry"]["coordinates"][1]

        t_log.info(f"Coordinates for {city}: {lat}/{long}")

        return {"city": city, "lat": lat, "long": long}

    # One dynamically mapped task instance per city in the list.
    cities_coordinates = get_lat_long_for_one_city.expand(city=cities)

    @task.branch
    def decide_timeframe(**context):
        if context["params"]["timeframe"] == "yesterday":
            return "get_weather_yesterday"
        elif context["params"]["timeframe"] == "today":
            return "get_weather_today"
        elif context["params"]["timeframe"] == "today_and_tomorrow":
            return "get_weather_today_and_tomorrow"
        else:
            raise ValueError("Invalid timeframe parameter.")

    get_weather_yesterday = HttpOperator(
        task_id="get_weather_yesterday",
        endpoint="forecast",
        method="GET",
        http_conn_id="weather_api_conn",
        log_response=True,
        data={
            "latitude": cities_coordinates["lat"],
            "longitude": cities_coordinates["long"],
            "hourly": "{{ params.weather_parameter }}",
            "past_days": 1,
        },
    )

    get_weather_today = HttpOperator(
        task_id="get_weather_today",
        endpoint="forecast",
        method="GET",
        http_conn_id="weather_api_conn",
        log_response=True,
        data={
            "latitude": cities_coordinates["lat"],
            "longitude": cities_coordinates["long"],
            "hourly": "{{ params.weather_parameter }}",
            "forecast_days": 1,
        },
    )

    get_weather_today_and_tomorrow = HttpOperator(
        task_id="get_weather_today_and_tomorrow",
        endpoint="forecast",
        method="GET",
        http_conn_id="weather_api_conn",
        log_response=True,
        data={
            "latitude": cities_coordinates["lat"],
            "longitude": cities_coordinates["long"],
            "hourly": "{{ params.weather_parameter }}",
            "forecast_days": 2,
        },
    )

    @task(
        trigger_rule="none_failed",
    )
    def get_weather_from_response(
        weather_yesterday: list,
        weather_today: list,
        weather_today_and_tomorrow: list,
        **context,
    ):
        if weather_yesterday:
            return weather_yesterday
        elif weather_today:
            return weather_today
        elif weather_today_and_tomorrow:
            return weather_today_and_tomorrow
        else:
            raise ValueError("No weather data found.")

    @task(outlets=[Dataset("current_weather_data")])
    def create_weather_table(
        weather: list | dict, cities_coordinates: list | dict, **context
    ):
        """
        Saves a table of the weather for the cities of interest to the logs and a CSV file.
        Args:
            weather: The weather data for the cities of interest, in JSON format.
            cities_coordinates: The coordinates of the cities of interest.
        """
        from airflow.models.xcom import LazyXComSelectSequence
        import json
        from tabulate import tabulate
        from include.helper_functions import (
            map_cities_to_weather,
        )
        import time

        time.sleep(context["params"]["simulate_task_delay"])

        weather = json.loads(weather)

        weather_parameter = context["params"]["weather_parameter"]

        weather = weather if isinstance(weather, list) else [weather]
        cities_coordinates = (
            list(cities_coordinates)
            if isinstance(cities_coordinates, LazyXComSelectSequence)
            else [cities_coordinates]
        )

        city_weather_info = map_cities_to_weather(
            weather, cities_coordinates, weather_parameter
        )

        t_log.info(
            tabulate(city_weather_info, headers="keys", tablefmt="grid", showindex=True)
        )

        return city_weather_info

    weather_data = get_weather_from_response(
        weather_today=get_weather_today.output,
        weather_today_and_tomorrow=get_weather_today_and_tomorrow.output,
        weather_yesterday=get_weather_yesterday.output,
    )

    create_weather_table(weather=weather_data, cities_coordinates=cities_coordinates)

    chain(
        cities_coordinates,
        decide_timeframe(),
        [get_weather_yesterday, get_weather_today, get_weather_today_and_tomorrow],
    )


upstream_dag_1_solution()
