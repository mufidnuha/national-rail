from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingest import ingest

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

ingest_from_s3 = PythonOperator(
    task_id='ingest_task',
    dag=dag,
    python_callable=ingest,
    op_kwargs={'date': date}
)
