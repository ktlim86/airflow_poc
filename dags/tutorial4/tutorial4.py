from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import timedelta, datetime
from airflow.utils.trigger_rule import TriggerRule
import csv
import os

"""
Tutorial 2
Code a dag to read file and then do a count of rows and print a console. 
"""
with DAG (dag_id="tutorial4", default_args={
        "depends_on_past":False,
        "email":"airflowtestemail@email.com",
        "email_on_failure":False,
        "email_on_success":False,
        "retries":1,
        "retry_delay": timedelta(minutes=5)
    },
    description="Tutorial 4 dag",
    schedule="*/1 * * * *",
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["tutorial4_tag"]) as dag:
    
    def wait_for_approval(**context):
        # Implement logic to wait for external trigger
        #print (context)
        print (context["dag_run"])
        dagrun:DagRun = context["dag_run"]
        ti = dagrun.get_task_instance(task_id="wait_for_approval")
        print ("ti=",ti)
        
        #print (dir(context["dag_run"]))
        print (dagrun.run_type,", trigger=",dagrun.external_trigger)
        print("Approval done.")

    t1 = BashOperator(task_id="t1",bash_command="echo 'Start Approval'")

    wait_for_approval_task = PythonOperator(
        task_id='wait_for_approval',
        python_callable=wait_for_approval,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE
    )

    t1 >> wait_for_approval_task