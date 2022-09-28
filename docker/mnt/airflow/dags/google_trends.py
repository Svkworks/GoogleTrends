import datetime
import pandas as pd
from pytrends.request import TrendReq
from google.cloud import storage
import os
import yaml

base_path = os.path.abspath(os.path.join(os.getcwd(), '..'))

config_file_path = base_path + '/utils/config.yaml'

with open(config_file_path, 'r+') as stream:
    yaml_dict = yaml.safe_load(stream=stream)

print(yaml_dict['keywords'])
print(base_path + yaml_dict['gcp_auth_path'])

"""
hl stands for hosting language for accessing Google Trends; in this example, we set English.
tz stands for timezone, in this example, we use the US time zone (represented in minutes), which is 360.
"""
pytrends = TrendReq()
"""build_payload :: The build_payload method from Pytrends is used to build a list of keywords you want to search in 
Google Trends 
"""
# max of 5 values allowed
search_list = yaml_dict['keywords']  # ["Oracle", "MySQL", "SQL Server", "PostgresSQL", "MongoDB"]
pytrends.build_payload(search_list, timeframe='today 5-y')
df_ot = pd.DataFrame(pytrends.interest_over_time()).drop(columns='isPartial')
df_ot = df_ot.reset_index(level=0)
current_date = datetime.datetime.today().strftime("%Y-%m-%d")
current_date_minus_3_years = (datetime.datetime.today() - datetime.timedelta(days=3 * 365)).strftime("%Y-%m-%d")
filtered_df = df_ot.loc[(df_ot['date'] >= current_date_minus_3_years) & (df_ot['date'] < current_date)]
# df = pd.DataFrame(data).drop(columns='isPartial')
print(filtered_df.head(10))


# Google Cloud Connectivity
# create storage client
gcp_auth_path = base_path + yaml_dict['gcp_auth_path']  # '/docker/mnt/airflow/utils/google_auth.json'
bucket_name = yaml_dict['bucket_name'] + '_123' # 'hd_ggl_trends'
upload_trends_path = yaml_dict['upload_trends_path']  # 'upload_trends/'

storage_client = storage.Client.from_service_account_json(gcp_auth_path)

# get bucket with name
bucket = storage_client.bucket(bucket_name)

if bucket.exists():
    bucket.blob(upload_trends_path + f'google_trends_{current_date}.parquet').upload_from_string(
        filtered_df.to_parquet())
else:
    new_bucket = storage_client.create_bucket(bucket, location="asia-south1")
    new_bucket.blob(upload_trends_path + f'google_trends_{current_date}.parquet').upload_from_string(
        filtered_df.to_parquet())

# df = pd.DataFrame(data=[{1, 2, 3}, {4, 5, 6}], columns=['a', 'b', 'c'])

