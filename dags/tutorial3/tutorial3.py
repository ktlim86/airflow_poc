from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
from glob import glob
from typing import Tuple
import csv
import os

"""
Tutorial 3
Code a dag to detect file drop and then pass the argument to the next operator to count the rows. 
"""
with DAG (dag_id="tutorial3", default_args={
        "depends_on_past":False,
        "email":"airflowtestemail@email.com",
        "email_on_failure":False,
        "email_on_success":False,
        "retries":1,
        "retry_delay": timedelta(minutes=5)
    },
    description="Tutorial 3 dag",
    schedule="@daily",
    start_date=datetime(2024,1,1),
    catchup=False,
    tags=["tutorial3_tag"]) as dag:
    t1 = BashOperator(task_id="t1",bash_command="echo 'Start processing data.'")
    
    def check_file_data (**kwargs)->int:
        _list = []
        for f in glob(os.path.join(kwargs.get("csv_folder"),"*")):
            _list.append (f)
        kwargs["ti"].xcom_push(key="file_list",value={"file_list":_list})
        return len(_list)
    
    def count_data (**kwargs)->Tuple[int,int]:
        _list = []
        csv_files = kwargs["ti"].xcom_pull (task_ids="f1",key="file_list")
        for csv_f in csv_files["file_list"]:
            with open (csv_f,"r") as f:
                reader = csv.DictReader(f)
                tmp_rows = [row for row in reader]
                _list.extend(tmp_rows)

        print (f"Number of rows processed is {len(_list)}. Number of files processed is {len(csv_files["file_list"])}.")
        return len(_list), len(csv_files["file_list"])

    csv_folder = os.path.join (os.getcwd(),"data")
    f1 = PythonSensor(task_id="f1",python_callable=check_file_data,poke_interval=30,mode="reschedule",op_kwargs={"csv_folder":csv_folder})
    t2 = PythonOperator(task_id="t2",python_callable=count_data)
    t3 = BashOperator(task_id="t3",bash_command="echo 'Completed processing data.'")
    t1 >> f1 >> t2 >> t3

