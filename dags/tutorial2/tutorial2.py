from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import os

"""
Tutorial 2
Code a dag to read file and then do a count of rows and print a console. 
"""
with DAG (dag_id="tutorial2", default_args={
        "depends_on_past":False,
        "email":"airflowtestemail@email.com",
        "email_on_failure":False,
        "email_on_success":False,
        "retries":1,
        "retry_delay": timedelta(minutes=5)
    },
    description="Tutorial 2 dag",
    schedule="@daily",
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["tutorial2_tag"]) as dag:
    t1 = BashOperator(task_id="t1",bash_command="echo 'Start processing data.'")
    
    def count_data (**kwargs)->int:
        _list = None
        csv_file = kwargs.get("csv_file")
        with open (csv_file,"r") as f:
            reader = csv.DictReader(f)
            _list = [row for row in reader]

        print (f"Number of files is {len(_list)}.")
        return len(_list)

    csv_file = os.path.join (os.getcwd(),"data","data.csv")
    t2 = PythonOperator(task_id="t2",python_callable=count_data,op_kwargs={"csv_file":csv_file})

    t3 = BashOperator(task_id="t3",bash_command="echo 'Completed processing data.'")

    t1 >> t2 >> t3

