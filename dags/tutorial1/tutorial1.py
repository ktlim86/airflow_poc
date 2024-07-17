from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

"""
Tutorial 1
Code a hello world dag to test out.
"""
with DAG (dag_id="tutorial1", default_args={
        "depends_on_past":False,
        "email":"airflowtestemail@email.com",
        "email_on_failure":False,
        "email_on_success":False,
        "retries":1,
        "retry_delay": timedelta(minutes=5)
    },
    description="Tutorial 1 dag",
    schedule_interval="@daily",
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["tutorial1_tag"]) as dag:
    t1 = BashOperator(task_id="t1",bash_command="echo 'Hello World'")
    t1
