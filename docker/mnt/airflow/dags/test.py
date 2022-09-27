# from datetime import datetime, timedelta
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.sensors.filesystem import FileSensor
# from airflow.models import Variable
# from airflow.utils import yaml
#
# #from utils.CodeWrapper import *
#
# with open('/opt/airflow/dags/config/config.yaml', 'r+') as stream:
# yaml_dict = yaml.safe_load(stream=stream)
#
# print('Prining the YAML file content')
# print(yaml_dict)
#
# # Postgres Table Name
# # table_name = yaml_dict['table_name']
# table_name = Variable.get('table_name')
#
# # Input File Path
# # input_file_path = yaml_dict['input_file_path']
# input_file_path = Variable.get("input_file_path") # Excercise 4
#
# # Output File Dir Path
# # output_file_directory = yaml_dict['out_file_directory']
# output_file_directory = Variable.get("output_file_directory") # Excercise 4
#
# # Postgres Connection Path
# # postgres_conn_id = yaml_dict['postgres_conn_id']
# # postgres_conn_id = BaseHook.get_connection("postgres_conn_id")
# postgres_conn_id = Variable.get("postgres_conn_id") # Excercise 4
#
#
# def write_data_to_postgres():
# """
# Function to write the data from pandas dataframe to postgresSQL table
# :return:
# """
# input_df = pd.read_csv(input_file_path)
# src = PostgresHook(postgres_conn_id=postgres_conn_id)
# # Create a list of tupples from the dataframe values
# tuples = [tuple(x) for x in input_df.to_numpy()]
# # SQL quert to execute
# src.insert_rows(table=table_name, rows=tuples)
# print('The table has been created successfully')
#
#
# def read_data_from_postgres():
# """
# Function to read the data from postgresSQL table and transform the data then load the final dataset to csv file
# :return:
# """
# # hook = PostgresHook(postgres_conn_id=postgres_conn_id)
# hook = BaseHook.get_hook(conn_id=postgres_conn_id) # Exercise - 4
# df = hook.get_pandas_df(sql=f"select * from {table_name};")
# print(f"output file size : {df.size}")
# # df.fillna(value='0', inplace=True)
# output_file_path = os.path.join(output_file_directory, yaml_dict['exercise_1c_path'])
# print(f' Output file path for 1c exercise : {output_file_path}')
# df.to_csv(output_file_path, index=False)
# print('The csv file has been created successfully in the output folder')
#
#
# ##################################### Exercise 2 ###########################################################
#
# def get_filtered_dataset(**kwargs):
# """
# Function to takes the csv file from exercise 1c and filtered the data based on product status desc is active
# and sales amount is greater than 0
# :param kwargs:
# :return: kwargs['ti']
# """
# input_file_path = os.path.join(output_file_directory, yaml_dict['exercise_1c_path'])
# input_df = pd.read_csv(input_file_path)
# new_df = input_df[(input_df['product_status_desc'] == 'Active') & (input_df['sales_amt'] > 0)]
# output_file_path_for_2a = os.path.join(output_file_directory, yaml_dict['exercise_2a_path'])
# print(f' Output file path for 2a exercise : {output_file_path_for_2a}')
# new_df.to_csv(output_file_path_for_2a, index=False)
# kwargs['ti'].xcom_push(key='input_file_path', value=output_file_path_for_2a)
# # kwargs['ti'].xcom_push(key='input_file_path_1', value=file_path)
# print('The filtered csv file has been created successfully in the output folder')
#
#
# def get_disctinct_clusters_sales(**kwargs):
# """
# Function to give the distinct cluster information along with the total sales
# :param kwargs:
# :return: None
# """
# file_path = kwargs['ti'].xcom_pull(key='input_file_path')
# # time.sleep(10)
# print(" File Path is :" + file_path)
# input_df = pd.read_csv(file_path)
# cluster_df = input_df[['cluster', 'sellthrough']]
# grouped_cluster_sell_df = cluster_df.groupby(['cluster'], as_index=False)['sellthrough'].sum()
# output_file_path_for_2b = os.path.join(output_file_directory, yaml_dict['exercise_2b_path'])
# grouped_cluster_sell_df.to_csv(output_file_path_for_2b, index=False)
# print('The distinct clusters sales csv file has been created successfully in the output folder')
#
#
# def get_netproft_products(**kwargs):
# """
# Function to calculate Net Profit (Sales - Net cost) and find top 10 products by Profit over whole business.
# :param kwargs:
# :return:
# """
# # time.sleep(10)
# file_path = kwargs['ti'].xcom_pull(key='input_file_path')
# print(" File Path is :" + file_path)
# input_df = pd.read_csv(file_path)
# cluster_df = input_df[['product_id', 'product_desc', 'deadnetprofit']]
# grouped_cluster_sell_df = cluster_df.groupby(['product_id', 'product_desc'], as_index=False)['deadnetprofit'].sum()
# grouped_cluster_sell_df.sort_values("deadnetprofit", axis=0, ascending=False,
# inplace=True, na_position='last')
# top_ten_products_df = grouped_cluster_sell_df.head(10)
# output_file_path_for_2c = os.path.join(output_file_directory, yaml_dict['exercise_2c_path'])
# top_ten_products_df.to_csv(output_file_path_for_2c, index=False)
# print('The Top 10 products as per the netprofit csv file has been created successfully in the output folder')
#
#
# def get_least_sold_subcategory_products(**kwargs):
# """
# Function to calculate the sales contribution of product across Subcategory,
# list down the products which have least contribution for every sub category.
# :param kwargs:
# :return:
# """
# file_path = kwargs['ti'].xcom_pull(key='input_file_path')
# print(" File Path is :" + file_path)
# input_df = pd.read_csv(file_path)
# subcategory_df = input_df[['product_id', 'product_desc', 'subcategory_name', 'sales_qty']]
# aggregates_subcategory_sales_df = subcategory_df.groupby(['product_id', 'product_desc', 'subcategory_name'],
# as_index=False)['sales_qty'].sum()
# aggregates_subcategory_sales_df.sort_values("sales_qty", axis=0, ascending=True,
# inplace=True, na_position='last')
# final_df = aggregates_subcategory_sales_df[['subcategory_name', 'product_id', 'product_desc', 'sales_qty']]
# output_file_path_for_2d = os.path.join(output_file_directory, yaml_dict['exercise_2d_path'])
# final_df.to_csv(output_file_path_for_2d, index=False)
#
#
# ################################################## Execrcise 5 ###################################################
#
# def get_max_profit_product_id(**kwargs):
# input_file_path = os.path.join(output_file_directory, yaml_dict['exercise_2c_path'])
# base_df = pd.read_csv(input_file_path)
# aggregated_product_id_df = base_df.groupby(['product_id'], as_index=False)['deadnetprofit'].sum()
# aggregated_product_id_df.sort_values(by='deadnetprofit', axis=0, ascending=False, inplace=True, na_position='last')
# max_profit_product_id_df = aggregated_product_id_df.head(1).to_dict('list')
# # max_profit_productid = max_profit_product_id_df['product_id'].values[0]
# print(f'max_profit_productid : {max_profit_product_id_df}')
# kwargs['ti'].xcom_push(key='max_profit_productid', value=max_profit_product_id_df)
#
#
# def get_sales_contrib_of_max_profit_productid(**kwargs):
# input_file_path_for_2d = os.path.join(output_file_directory, yaml_dict['exercise_2d_path'])
# base_sales_df = pd.read_csv(input_file_path_for_2d)[['product_id', 'sales_qty']]
# max_profit_product_id = kwargs['ti'].xcom_pull(key='max_profit_productid')['product_id'][0]
# print(f' type of max_profit_product_id {type(max_profit_product_id)} and value {max_profit_product_id}')
# aggregated_sales_df = base_sales_df.loc[base_sales_df['product_id'] == max_profit_product_id] \
# .groupby(['product_id'], as_index=False)['sales_qty'].sum()
# # final_df = pd.merge(left=aggregated_sales_df, right=max_profit_product_id_df, on='product_id', how='inner')
# output_file_path_for_5b = os.path.join(output_file_directory, yaml_dict['exercise_5b_path'])
# aggregated_sales_df.to_csv(output_file_path_for_5b, index=False)
#
#
# default_args = dict(owner="airflow", email_on_failure=False,
# email_on_retry=False, email="svk041994@gmail.com", retry_delays=timedelta(minutes=5))
#
# # Create DAG context manager
# with DAG(start_date=datetime(2021, 1, 1), dag_id="bcg_apache_airflow_caselet_datapipeline",
# schedule_interval="@daily", default_args=default_args, catchup=False) as dag:
# create_caselet_table = PostgresOperator(
# sql='sql/create_caselet_table_schema.sql',
# task_id="create_caselet_table",
# postgres_conn_id='postgres_default')
#
# load_data_from_csv_to_table = PythonOperator(
# task_id='load_data_from_csv_to_table',
# python_callable=write_data_to_postgres)
#
# save_data_from_table_to_csv = PythonOperator(
# task_id='save_data_from_table_to_csv',
# python_callable=read_data_from_postgres)
#
# is_file_available = FileSensor(
# task_id='is_file_available',
# fs_conn_id='file_path',
# filepath='CaseletDataset_output.csv',
# poke_interval=5,
# timeout=30)
#
# get_filtered_dataset = PythonOperator(
# task_id='get_filtered_dataset',
# python_callable=get_filtered_dataset,
# provide_context=True)
#
# get_disctinct_clusters_sales = PythonOperator(
# task_id='get_disctinct_clusters_sales',
# python_callable=get_disctinct_clusters_sales,
# provide_context=True)
#
# get_netproft_products = PythonOperator(
# task_id='get_netproft_products',
# python_callable=get_netproft_products,
# provide_context=True)
#
# get_least_sold_subcategory_products = PythonOperator(
# task_id='get_least_sold_subcategory_products',
# python_callable=get_least_sold_subcategory_products,
# provide_context=True)
# # Exercise 5a
# get_max_profit_product_id = PythonOperator(
# task_id='get_max_profit_product_id',
# python_callable=get_max_profit_product_id,
# provide_context=True)
# # Exercise 5b
# get_sales_contrib_of_max_profit_productid = PythonOperator(
# task_id='get_sales_contrib_of_max_profit_productid',
# python_callable=get_sales_contrib_of_max_profit_productid,
# provide_context=True)
#
# create_caselet_table >> load_data_from_csv_to_table >> save_data_from_table_to_csv >> is_file_available \
# >> get_filtered_dataset >> [get_disctinct_clusters_sales, get_netproft_products, get_least_sold_subcategory_products]
# get_filtered_dataset >> get_max_profit_product_id >> get_sales_contrib_of_max_profit_productid