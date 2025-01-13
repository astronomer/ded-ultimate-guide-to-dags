"""
This DAG shows examples for different DAG parameters.

See https://www.astronomer.io/docs/learn/airflow-dag-parameters/ for an extensive list.
"""

from airflow.decorators import dag, task
from airflow.models.param import Param
from pendulum import datetime, duration


@dag(
    dag_id="dag_parameters",  # the DAG id is the unique identifier for the DAG, if not set, the name of the decorated function will be used
    dag_display_name="DAG Parameters 🚀",  # the name displayed in the Airflow UI, can include special characters and emojis
    start_date=datetime(
        2024, 11, 1
    ),  # date after which the DAG can be scheduled, see: https://www.astronomer.io/docs/learn/scheduling-in-airflow/#scheduling-concepts
    schedule="@daily",  # the DAG's schedule, there are many options, see: https://www.astronomer.io/docs/learn/scheduling-in-airflow for options
    catchup=False,  # whether or not missed runs should be scheduled upon unpausing of the DAG, see: https://www.astronomer.io/docs/learn/rerunning-dags#catchup
    max_active_runs=2,  # maximum number of active DAG runs at any point in time
    max_consecutive_failed_dag_runs=5,  # auto-pauses the DAG after x consecutive failed runs, experimental
    max_active_tasks=10, # maximum number of active tasks across all runs of this DAG at any point in time
    dagrun_timeout=duration(hours=2),  # timeout duration for runs of this DAG
    fail_stop=True,  # fails the whole DAG as soon as any task fails, can only be used with the "all_success" trigger rule
    description="Show several DAG params",  # description of the DAG next to the name in the UI
    doc_md=__doc__,  # add DAG Docs in the UI in markdown, see https://www.astronomer.io/docs/learn/custom-airflow-ui-docs-tutorial
    default_args={
        "owner": "Astro",  # owner of this DAG in the Airflow UI
        "retries": 3,  # tasks retry 3 times before they fail
        "retry_delay": duration(seconds=30),  # tasks wait 30s in between retries
        "retry_exponential_backoff": True,  # wait longer between retries with each attempt
    },
    owner_links={
        "Astro": "https://www.astronomer.io/docs/"
    },  # add links to the owner in the UI
    tags=["syntax_example", "parameters"],  # add tags in the UI
    params={
        "my_string_param": Param(
            "Airflow is awesome!",
            type="string",
            title="Favorite orchestrator:",
            description="Enter your favorite data orchestration tool.",
            section="Important params",
            minLength=1,
            maxLength=200,
        ),
        "my_datetime_param": Param(
            "2016-10-18T14:00:00+00:00",
            type="string",
            format="date-time",
        ),
        "my_enum_param": Param(
            "Hi :)", type="string", enum=["Hola :)", "Hei :)", "Bonjour :)", "Hi :)"]
        ),
        "my_bool_param": Param(True, type="boolean"),
    },  # Airflow params can add interactive options on manual runs. See: https://www.astronomer.io/docs/learn/airflow-params
)
def dag_parameters():

    @task
    def print_the_params(**context):
        my_string_param = context["params"]["my_string_param"]
        my_datetime_param = context["params"]["my_datetime_param"]
        my_enum_param = context["params"]["my_enum_param"]
        my_bool_param = context["params"]["my_bool_param"]

        print(f"my_string_param: {my_string_param}")
        print(f"my_datetime_param: {my_datetime_param}")
        print(f"my_enum_param: {my_enum_param}")
        print(f"my_bool_param: {my_bool_param}")

    print_the_params()


dag_parameters()
