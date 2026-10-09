"""
HW2 Q1.2 - HelloWorld DAG
Designed for Airflow 2.x with SequentialExecutor.
"""

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def say_hello():
    print("Hello World from EECS E6893 HW2!")


with DAG(
    dag_id="hw2_hello_world",
    description="HW2 Q1.2 HelloWorld with SequentialExecutor",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    schedule=None,
    catchup=False,
    tags=["EECS6893", "HW2"],
) as dag:

    hello = PythonOperator(
        task_id="hello",
        python_callable=say_hello,
    )

    show_time = BashOperator(
        task_id="show_time",
        bash_command='echo "Current time:" && date',
    )

    finish = BashOperator(
        task_id="finish",
        bash_command='echo "HelloWorld DAG completed successfully."',
    )

    hello >> show_time >> finish
