from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os
from ingest import ingest
from etl.extract.extract_ref import extract_ref

date = datetime.now().strftime('%Y%m%d')

default_args = {
    'owner': 'mufida',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 24),
    'email': ['mufidanuha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='national_rail_dag',
    default_args=default_args,
    schedule_interval='@daily'
)

create_file_dir_task = BashOperator(
    task_id='create_file_dir',
    dag=dag,
    bash_command='mkdir {dir}/PPTimetable/{date}'.format(dir=os.getcwd(), date=date)
)

ingest_task = PythonOperator(
    task_id='ingest_from_s3',
    dag=dag,
    python_callable=ingest,
    op_kwargs={'date': date}
)

unzip_file_task = BashOperator(
    task_id='unzip_file',
    dag=dag,
    bash_command='gzip -d {dir}/PPTimetable/{date}/{date}*.xml.gz'.format(dir=os.getcwd(), date=date)
)

extract_ref_task = PythonOperator(
    task_id='extract_ref',
    dag=dag,
    python_callable=extract_ref,
)

create_file_dir_task >> ingest_task >> unzip_file_task


