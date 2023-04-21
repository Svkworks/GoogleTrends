from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.utils.edgemodifier import Label

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

DAG_ID = "example_dbt_cloud"

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

with DAG(
    dag_id=DAG_ID,
    default_args={"dbt_cloud_conn_id": "dbt_conn"},
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")


    trigger_job_run1 = DbtCloudRunJobOperator(
        task_id="trigger_job_run1",
        job_id=5947,
        check_interval=10,
        timeout=300,
    )


    begin >> Label("Dbt cloud Run Job") >> trigger_job_run1 >> end