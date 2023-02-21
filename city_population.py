import json
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator


def extract(ti=None, **kwargs):
    """
    Pushes the estimated population (in millions) of
    various cities into XCom for the ETL pipeline.
    Obviously in reality this would be fetching this
    data from some source, not hardcoded values.
    """
    sample_data = {"Tokyo": 3.7, "Jakarta": 3.3, "Delhi": 2.9}
    ti.xcom_push("city_populations", json.dumps(sample_data))


def transform(ti=None, **kwargs):
    """
    Pulls the provided raw data from XCom and pushes
    the name of the largest city in the set to XCom.
    """
    raw_data = ti.xcom_pull(task_ids="extract", key="city_populations")
    data = json.loads(raw_data)

    largest_city = max(data, key=data.get)
    ti.xcom_push("largest_city", largest_city)


def load(ti=None, **kwargs):
    """
    Loads and prints the name of the largest city in
    the set as determined by the transform.
    """
    largest_city = ti.xcom_pull(task_ids="transform", key="largest_city")

    print(largest_city)


with DAG(
        dag_id="city_pop_etl_pythonoperator",
        schedule=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["xcom"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    chain(
        extract_task,
        transform_task,
        load_task,
    )
