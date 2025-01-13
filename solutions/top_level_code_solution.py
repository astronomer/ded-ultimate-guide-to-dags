from datetime import datetime

from airflow.decorators import dag, task

from include.helper_functions import expensive_api_call


@dag(
    dag_display_name="Solution: top_level_code",
    start_date=datetime(2023, 1, 1),
    max_active_runs=3,
    schedule=None,
    catchup=False,
    tags=["solution"],
    default_args={"owner": "Astro", "retries": 3},
)
def top_level_code_solution():

    @task
    def reveal_the_meaning_of_life_the_universe_and_everything():
        the_answer = expensive_api_call()
        print(f"The meaning of life, the universe, and everything is... {the_answer}.")

    reveal_the_meaning_of_life_the_universe_and_everything()


top_level_code_solution()
