"""
### DAG that performs basic math operations

This DAG is a simple example of how to use a custom operator to perform basic 
math operations.
Demo: dag.test() and Airflow testing
"""

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from pendulum import datetime

from include.custom_operator import MyBasicMathOperator
from include.helper_functions import get_random_number_from_api


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    params={
        "upper_limit": Param(100, type="integer"),
        "lower_limit": Param(1, type="integer"),
    },
    default_args={"retries": 3},
    tags=["syntax_example"],
)
def custom_operator_dag_test():

    @task
    def pick_a_random_number(**context) -> int:
        "Return a random number within the limits."
        minimum = context["params"]["lower_limit"]
        maximum = context["params"]["upper_limit"]

        num = get_random_number_from_api(
            min=minimum,
            max=maximum,
            count=1,
        )

        return num

    pick_a_random_number_obj = pick_a_random_number()

    operate_with_23 = MyBasicMathOperator(
        task_id="operate_with_23",
        first_number=pick_a_random_number_obj,
        second_number=23,
        operation="+",
    )

    chain(
        pick_a_random_number_obj,
        operate_with_23,
    )


dag_obj = custom_operator_dag_test()


if __name__ == "__main__":
    # conn_path = "dag_test/connections.yaml"
    # variables_path = "dag_test/variables.yaml"
    upper_limit = 20
    lower_limit = 10

    dag_obj.test(
        execution_date=datetime(2025, 2, 1),
        # conn_file_path=conn_path,
        # variable_file_path=variables_path,
        run_conf={"upper_limit": upper_limit, "lower_limit": lower_limit},
    )
