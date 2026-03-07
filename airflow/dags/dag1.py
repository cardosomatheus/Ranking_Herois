from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.decorators import task


# A Dag represents a workflow, a collection of tasks
with DAG(dag_id="demo", 
         start_date=datetime(2022, 1, 1),
         schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow ")

    @task()
    def marilan():
        print("MARILANNNNNNNNNN")


    # Set dependencies between tasks
    hello >> airflow() >> marilan()
