from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "local_executor_demo",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
) as dag:
    local_task = BashOperator(
        task_id="local_task",
        bash_command="echo 'local task running in scheduler pod'",
    )
