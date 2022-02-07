from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from etl.ingest.ingest_from_s3 import ingest

#date = datetime.now().strftime('%Y%m%d')
date = '20220206'
landing_path = '{root_path}/mnt/data_lake/landing/PPTimetable'.format(root_path=os.getcwd())
clean_path = '{root_path}/mnt/data_lake/clean/PPTimetable'.format(root_path=os.getcwd())
extract_ref_path = '{root_path}/etl/extract/extract.py'.format(root_path=os.getcwd())

default_args = {
    'owner': 'mufida',
    'depends_on_past': False,
    'start_date': datetime(2022, 2, 7),
    'email': ['mufidanuha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='coba_dag',
    default_args=default_args,
    schedule_interval='@daily'
)

extract_ref_task = SparkSubmitOperator(
    task_id='extract_ref',
    conn_id='spark_local',
    dag=dag,
    packages='com.databricks:spark-xml_2.12:0.12.0',
    application=extract_ref_path,
    conf={"spark.master":'local[*]'}
)

#create_landing_dir_task >> create_clean_dir_task >> ingest_task >> unzip_file_task >> 
extract_ref_task

