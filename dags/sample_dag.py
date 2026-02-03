from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 2, 3),
    "retries": 1,
}


with DAG(
    dag_id="databricks_churn_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
    tags=["databricks", "customer_churn"],
) as dag:
    
    # write python operator to copy csv from local path to unity catalog table.

    transform_data = DatabricksSubmitRunOperator(
        task_id="transform_customer_churn",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "transform_customer_churn",
            "tasks": [
                {
                    "task_key": "transform_customer_churn",
                    "spark_python_task": {
                        "python_file": "dbfs:/Workspace/Users/npatel17da@gmail.com/transform_customer_churn.py",
                        "parameters": [
                            "/Workspace/Users/npatel17da@gmail.com/customer_churn_data.csv",
                            "{{ execution_date.strftime('%Y%m%d%H') }}" # using execution timestamp for idempotency
                        ]
                    },
                    "environment_key": "default"
                }
            ],
            "environments": [
                {
                    "environment_key": "default",
                    "spec": {
                        "environment_version": "4"
                    }
                }
            ]
        }
    )

    transform_data