from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum

from extractor.parser import DatabaseWriter

def write_to_database():
    writer = DatabaseWriter('1C_827496_20231112.json')
    writer.write_to_database()

with DAG(
        "exctractor_loader_v1",
        default_args={"retries": 2},
        description="DAG tutorial",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example"],
) as dag:

    dag.doc_md = __doc__

    test_insert_task = PythonOperator(
        task_id="write_to_database",
        python_callable=write_to_database,
    )

    test_insert_task