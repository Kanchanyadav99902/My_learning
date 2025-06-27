from __future__ import annotations
from airflow import DAG
from datetime import datetime

from airflow.decorators import  task

with DAG(dag_id="example_dynamic_task_mapping", schedule=None, start_date=datetime(2022, 3, 4)) as dag:

    @task
    def add_one(x: int):
        return x + 1

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    added_values = add_one.expand(x=[1, 2, 3])
    sum_it(added_values)

with DAG(
    dag_id="example_task_mapping_second_order", schedule=None, catchup=False, start_date=datetime(2022, 3, 4)
) as dag2:

    @task
    def get_nums():
        return [1, 2, 3]

    @task
    def times_2(num):
        return num * 2

    @task
    def add_10(num):
        return num + 10

    _get_nums = get_nums()
    _times_2 = times_2.expand(num=_get_nums)
    add_10.expand(num=_times_2)