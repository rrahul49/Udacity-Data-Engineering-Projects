import os
import configparser
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

from operators import (CopyToRedshiftOperator, SASToRedshiftOperator, DataQualityOperator)

from helpers import sas_data, s3_keys
from helpers import create_tables


config = configparser.ConfigParser()
config.read('dwh.cfg')

REDSHIFT_ARN = config['CLUSTER']['ARN']
S3_BUCKET = config['S3']['BUCKET']


default_args = {
    'owner': 'rahulravi',
    'start_date': datetime(2019, 1, 12),
    'retries':  3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depends_on_past': False,
}

dag = DAG('capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_Execution',  dag=dag)

load_operator = DummyOperator(task_id='Begin_load',  dag=dag)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

create_immigration_table = PostgresOperator(
    task_id= "create_immigration_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_immigration
)

create_us_cities_demographics_table = PostgresOperator(
    task_id= "create_us_cities_demographics_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_us_cities_demographics
)

create_airport_codes_table = PostgresOperator(
    task_id= "create_airport_codes_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_airport_codes
)

create_world_temperature_table = PostgresOperator(
    task_id= "create_world_temperature_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_world_temperature
)

create_i94cit_res_table = PostgresOperator(
    task_id= "create_i94cit_res_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_i94cit_res
)

create_i94port_table = PostgresOperator(
    task_id= "create_i94port_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_i94port
)

create_i94mode_table = PostgresOperator(
    task_id= "create_i94mode_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_i94mode
)

create_i94addr_table = PostgresOperator(
    task_id= "create_i94addr_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_i94addr
)

create_i94visa_table = PostgresOperator(
    task_id= "create_i94visa_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables.create_i94visa
)

start_operator >> [create_immigration_table,create_us_cities_demographics_table,
                   create_airport_codes_table,create_world_temperature_table,
                   create_i94cit_res_table,create_i94port_table,create_i94mode_table,
                   create_i94addr_table,create_i94visa_table]

[create_immigration_table,create_us_cities_demographics_table,
 create_airport_codes_table,create_world_temperature_table,
 create_i94cit_res_table,create_i94port_table,create_i94mode_table,
 create_i94addr_table,create_i94visa_table] >> load_operator

for table in s3_keys:
  copy_from_s3_task = CopyToRedshiftOperator(
    task_id=f'copy_{table["name"]}_from_s3',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    role_arn=REDSHIFT_ARN,
    table=table['name'],
    s3_bucket='udacity-capstone-rr',
    s3_key=table['key'],
    file_format=table['file_format'],
    delimiter=table['sep']
  )

  check_task = DataQualityOperator(
    task_id=f'quality_check_{table["name"]}_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=table['name'],
    quality_check=table['quality_check']
  )

  load_operator >> copy_from_s3_task
  copy_from_s3_task >> check_task
  check_task >> end_operator


for table in sas_data:
  load_from_sas_task = SASToRedshiftOperator(
    task_id=f'load_{table["name"]}_from_sas_source_code',
    dag=dag,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table=table['name'],
    s3_bucket='udacity-capstone-rr',
    s3_key='data/I94_SAS_Labels_Descriptions.SAS',
    sas_value=table['value'],
    columns=table['columns']
  )

  check_task = DataQualityOperator(
    task_id=f'quality_check_{table["name"]}_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=table['name'],
    quality_check=table['quality_check']
  )

  load_operator >> load_from_sas_task
  load_from_sas_task >> check_task
  check_task >> end_operator
