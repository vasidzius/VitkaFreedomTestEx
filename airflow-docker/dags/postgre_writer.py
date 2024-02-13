from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from test_insert.test_insert import PostgreSQLConnector

import pendulum

with DAG(
        "test_insert_dag",
        # [START default_args]
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={"retries": 2},
        # [END default_args]
        description="DAG tutorial",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example"],
) as dag:

    dag.doc_md = __doc__

    def write_to_database():
        connector = PostgreSQLConnector(
            dbname="postgres",
            user="postgres",
            password="",
            host=""
        )
        connector.write_to_db(123, "example text")


    test_insert_task = PythonOperator(
        task_id="write_to_database",
        python_callable=write_to_database,
    )

    test_insert_task
