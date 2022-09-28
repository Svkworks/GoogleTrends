import os
import yaml
import pandas as pd
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pytrends.request import TrendReq
from google.cloud import storage


base_path = os.path.abspath(os.path.join(os.getcwd()))

config_file_path = base_path + '/utils/config.yaml'

with open(config_file_path, 'r+') as stream:
    yaml_dict = yaml.safe_load(stream=stream)


def google_trends_to_gcs():
    """
    Function to write the data from Google trends to google cloud storage bucket
    :return:
    """
    pytrends = TrendReq()
    search_list = yaml_dict['keywords']  # ["Oracle", "MySQL", "SQL Server", "PostgresSQL", "MongoDB"]
    pytrends.build_payload(search_list, timeframe='today 5-y')
    df_ot = pd.DataFrame(pytrends.interest_over_time()).drop(columns='isPartial')
    df_ot = df_ot.reset_index(level=0)
    current_date = datetime.today().strftime("%Y-%m-%d")
    current_date_minus_3_years = (datetime.today() - timedelta(days=3 * 365)).strftime("%Y-%m-%d")
    filtered_df = df_ot.loc[(df_ot['date'] >= current_date_minus_3_years) & (df_ot['date'] < current_date)]
    # df = pd.DataFrame(data).drop(columns='isPartial')

    # create storage client
    gcp_auth_path = base_path + yaml_dict['gcp_auth_path']  # '/docker/mnt/airflow/utils/google_auth.json'
    bucket_name = yaml_dict['bucket_name']  # 'hd_ggl_trends'
    upload_trends_path = yaml_dict['upload_trends_path']  # 'upload_trends/'

    storage_client = storage.Client.from_service_account_json(gcp_auth_path)

    bucket = storage_client.bucket(bucket_name)

    if bucket.exists():
        bucket.blob(upload_trends_path + f'google_trends_{current_date}.parquet').upload_from_string(
            filtered_df.to_parquet())
    else:
        new_bucket = storage_client.create_bucket(bucket, location="asia-south1")
        new_bucket.blob(upload_trends_path + f'google_trends_{current_date}.parquet').upload_from_string(
            filtered_df.to_parquet())

    print(f'The Google Trends File has been moved to Google cloud bucket {bucket_name}')


default_args = dict(owner="airflow", email_on_failure=False, email_on_retry=False, email="svk041994@gmail.com",
                    retry_delays=timedelta(minutes=5))

with DAG(start_date=datetime(2021, 1, 1), dag_id='google_trends_datapipeline', schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:
    start_task = BashOperator(
        task_id="Start",
        bash_command="echo This is Start Task",
        dag=dag
    )

    google_trends_to_gcs = PythonOperator(
        task_id='google_trends_to_gcs',
        python_callable=google_trends_to_gcs
    )

    end_task = BashOperator(
        task_id="End",
        bash_command="echo This is End Task",
        dag=dag
    )

start_task >> google_trends_to_gcs >> end_task
