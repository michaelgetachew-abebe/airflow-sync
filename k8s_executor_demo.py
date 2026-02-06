  from datetime import datetime
  from airflow import DAG
  from airflow.operators.bash import BashOperator
  from kubernetes.client import models as k8s

  with DAG(
      "k8s_executor_demo",
      start_date=datetime(2024, 1, 1),
      schedule="*/1 * * * *",
      catchup=False,
  ) as dag:

      local_task = BashOperator(
          task_id="local_task",
          bash_command="echo 'Running locally in scheduler pod'",
      )

      k8s_task = BashOperator(
          task_id="k8s_task",
          bash_command="echo 'Running in a separate K8s pod'",
          queue="kubernetes",  # <-- THIS IS THE KEY
          executor_config={
              "pod_override": k8s.V1Pod(
                  spec=k8s.V1PodSpec(
                      containers=[
                          k8s.V1Container(
                              name="base",
                              image="apache/airflow:2.11.0",
                              resources=k8s.V1ResourceRequirements(
                                  requests={"cpu": "100m", "memory": "256Mi"},
                                  limits={"cpu": "500m", "memory": "512Mi"}
                              )
                          )
                      ]
                  )
              )
          },
      )

      local_task >> k8s_task
