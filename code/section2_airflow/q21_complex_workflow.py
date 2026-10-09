"""
EECS E6893 Big Data Analytics - HW2 Q2.1
Complex Airflow DAG matching the assignment / Oct. 2 TA tutorial diagram.

PythonOperator nodes (yellow):
    t4, t7, t8, t11, t12, t15, t16, t19

BashOperator nodes (gray):
    t1, t2, t3, t5, t6, t9, t10, t13, t14, t17, t18

The DAG is scheduled every 30 minutes with SequentialExecutor configured
outside this file.  A fixed past start_date + catchup=False prevents a large
historical backfill while allowing the scheduler to create the current/latest
eligible run after the DAG is enabled.
"""

from datetime import timedelta
import time
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# ---------------------------------------------------------------------
# Python callables: use multiple kinds of Python work, as requested.
# ---------------------------------------------------------------------
def py_print(task_name, message):
    print(f"{task_name}: {message}")


def py_count(task_name, items):
    print(f"{task_name}: count = {len(items)}")


def py_sleep(task_name, seconds=2):
    print(f"{task_name}: sleeping for {seconds} seconds")
    time.sleep(seconds)
    print(f"{task_name}: awake")


def py_sum(task_name, numbers):
    print(f"{task_name}: sum = {sum(numbers)}")


def py_stats(task_name, numbers):
    avg = sum(numbers) / len(numbers)
    print(f"{task_name}: min={min(numbers)}, max={max(numbers)}, avg={avg:.2f}")


with DAG(
    dag_id="hw2_q21_complex_workflow",
    description="HW2 Q2.1 t1-t19 workflow from the assignment/tutorial",
    start_date=pendulum.datetime(2026, 10, 1, 0, 0, tz="America/New_York"),
    # TA tutorial uses timedelta for schedule_interval; HW requires 30 minutes.
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    max_active_runs=1,
    tags=["EECS6893", "HW2", "Q2.1"],
) as dag:

    # ---------------------------
    # BashOperator tasks (gray)
    # ---------------------------
    t1 = BashOperator(
        task_id="t1",
        bash_command='echo "t1: workflow started"',
    )

    t2 = BashOperator(
        task_id="t2",
        bash_command='echo "t2: current date/time" && date',
    )

    t3 = BashOperator(
        task_id="t3",
        bash_command='python3 -c "print(\'t3: Python command launched from BashOperator\')"',
    )

    t5 = BashOperator(
        task_id="t5",
        bash_command='printf "t5: bash printf command\\n"',
    )

    t6 = BashOperator(
        task_id="t6",
        bash_command='echo "t6: working directory is $(pwd)"',
    )

    t9 = BashOperator(
        task_id="t9",
        bash_command='echo "t9: counting words" && echo "big data analytics airflow" | wc -w',
    )

    t10 = BashOperator(
        task_id="t10",
        bash_command='echo "t10: arithmetic result = $((7 * 6))"',
    )

    t13 = BashOperator(
        task_id="t13",
        bash_command='echo "t13: listing first few files" && ls | head -5',
    )

    t14 = BashOperator(
        task_id="t14",
        bash_command='echo "t14: merge point reached"',
    )

    t17 = BashOperator(
        task_id="t17",
        bash_command='echo "t17: hostname is $(hostname)"',
    )

    t18 = BashOperator(
        task_id="t18",
        bash_command='echo "t18: downstream merge completed"',
    )

    # -----------------------------
    # PythonOperator tasks (yellow)
    # -----------------------------
    t4 = PythonOperator(
        task_id="t4",
        python_callable=py_print,
        op_kwargs={"task_name": "t4", "message": "independent Python print branch"},
    )

    t7 = PythonOperator(
        task_id="t7",
        python_callable=py_count,
        op_kwargs={"task_name": "t7", "items": ["a", "b", "c", "d", "e"]},
    )

    t8 = PythonOperator(
        task_id="t8",
        python_callable=py_sleep,
        op_kwargs={"task_name": "t8", "seconds": 2},
    )

    t11 = PythonOperator(
        task_id="t11",
        python_callable=py_sum,
        op_kwargs={"task_name": "t11", "numbers": [1, 2, 3, 4, 5]},
    )

    t12 = PythonOperator(
        task_id="t12",
        python_callable=py_stats,
        op_kwargs={"task_name": "t12", "numbers": [3, 7, 11, 15, 19]},
    )

    t15 = PythonOperator(
        task_id="t15",
        python_callable=py_print,
        op_kwargs={"task_name": "t15", "message": "branch from t8 finished"},
    )

    t16 = PythonOperator(
        task_id="t16",
        python_callable=py_count,
        op_kwargs={"task_name": "t16", "items": list(range(10))},
    )

    t19 = PythonOperator(
        task_id="t19",
        python_callable=py_print,
        op_kwargs={"task_name": "t19", "message": "all terminal branches completed"},
    )

    # -----------------------------------------------------------------
    # Dependencies rechecked against the Oct. 2 TA tutorial, page 10.
    # -----------------------------------------------------------------
    t1 >> [t2, t3, t4, t5]
    t2 >> t6
    t3 >> [t7, t12]
    t5 >> [t8, t9]
    t8 >> [t10, t15]
    t9 >> [t11, t12]
    [t10, t11, t12, t7] >> t14
    t7 >> [t13, t18]
    t14 >> [t16, t17]
    [t15, t13, t17] >> t18
    [t16, t18] >> t19
