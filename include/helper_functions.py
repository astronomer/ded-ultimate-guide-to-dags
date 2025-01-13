from time import sleep

import pandas as pd


def map_cities_to_weather(
    forecasts: list, cities_coordinates: list, type_of_forecast: str
) -> pd.DataFrame:
    """
    Maps each city to its corresponding weather data based on the forecast type.

    Parameters:
    - forecasts: A list of dictionaries containing weather data.
    - cities_coordinates: A list of dictionaries containing city names and their coordinates.
    - type_of_forecast: A string indicating the type of forecast to extract from the hourly data.

    Returns:
    - A pandas DataFrame containing the weather data for each city for each timestamp.
    """
    list_of_cities = [city["city"] for city in cities_coordinates]
    timestamps = forecasts[0]["hourly"]["time"]

    df = pd.DataFrame(columns=list_of_cities, index=timestamps)

    threshold = 0.05

    for city in cities_coordinates:
        city_name = city["city"]
        city_lat = city["lat"]
        city_long = city["long"]

        for forecast in forecasts:
            if (
                abs(forecast["latitude"] - city_lat) < threshold
                and abs(forecast["longitude"] - city_long) < threshold
            ):
                for i, time in enumerate(forecast["hourly"]["time"]):
                    df.at[time, city_name] = forecast["hourly"][type_of_forecast][i]

    return df


def get_random_number_from_api(min: int, max: int, count: int) -> int:
    import requests

    r = requests.get(
        f"http://www.randomnumberapi.com/api/v1.0/random?min={min}&max={max}&count={count}"
    )

    return r.json()[0]


def expensive_api_call():
    """
    Returns the answer to the question "What is the meaning of life, the universe, and everything?"
    """
    sleep(1)  # imagine this is a very expensive operation, you can test it by changing the sleep time, at some point your DAGs will stop parsing correctly!
    return 42
