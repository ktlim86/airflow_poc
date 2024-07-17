# Instruction

Install airflow by doing `pip install apache-airflow`
Run `airflow standalone` to initialize. Default path will be /Users/<<USERNAME>>/airflow.
Change `dags_folder` in airflow.cfg to the path that you want.

Upon finishing creating the dag, use another console to run the following command:

1. `python main.py` to see if there is any error.
2. `airflow scheduler` to register the dag.
3. `airflow dags test <<DAG_ID>>` to test run the dag.

## Tutorial 1

Code a hello world dag to test out.

## Tutorial 2

Code a dag to read file and then do a count of rows and print a console. Use the python in helper/mock_data.py to generate some test data.

## Tutorial 3

Code a dag to pass the XCom to the next operator to count the rows. 