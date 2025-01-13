"""
# Toy DAG scheduled to run on an update to one dataset in each of 2 groups
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime


@dag(
    start_date=datetime(2024, 11, 1),
    schedule=(
        (Dataset("dataset1") | Dataset("dataset2"))
        & (Dataset("dataset3") | Dataset("dataset4"))
    ),  # Runs when one dataset in each group is updated
    # NEW in Airflow 2.9: Use conditional logic to schedule a DAG based on datasets
    # Use () instead of [] to be able to use conditional dataset scheduling!
    catchup=False,
    doc_md=__doc__,
    tags=["syntax_example"],
    default_args={"retries": 3},
)
def conditional_dataset_schedule():
    @task
    def say_hello():
        print("Hello")

    say_hello()


conditional_dataset_schedule()