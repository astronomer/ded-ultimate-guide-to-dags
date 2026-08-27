"""WARNING BAD PRACTICE: DO NOT USE TOP LEVEL CODE IN YOUR DAG FILES"""

from airflow.sdk import dag, task
from pendulum import datetime

from include.helper_functions import expensive_trajectory_calculation


@dag(
    dag_display_name="4. Exercise: top_level_code",
    start_date=datetime(2026, 6, 1),
    max_active_runs=3,
    schedule=None,
    tags=["exercise", "exercise_4"],
)
def top_level_code():

    #### EXERCISE 4 ####
    the_optimal_trajectory_correction = expensive_trajectory_calculation()

    @task
    def reveal_the_trajectory_correction(correction):
        print(f"The optimal trajectory correction is... {correction} degrees.")

    reveal_the_trajectory_correction(the_optimal_trajectory_correction)


top_level_code()
