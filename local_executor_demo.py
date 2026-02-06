from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "k8s_executor_demo",
    start_date=datetime(2024, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
) as dag:
    k8s_task = BashOperator(
        task_id="k8s_task",
        bash_command="echo 'k8s task running in a pod'",
        executor_config={
            "KubernetesExecutor": {
                "image": "apache/airflow:2.11.0"
            }
        },
    )
