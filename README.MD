curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'

чтобы запустить
- из директории airflow-docker запустить docker-compose up

обязательно видео посмотреть - https://www.youtube.com/watch?v=aTaytcxy2Ck

в UI airflow найти test_insert_dag и запустить, код в  [postgre_writer.py](airflow-docker%2Fdags%2Fpostgre_writer.py)>