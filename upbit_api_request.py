from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from lib.Screening import Screening
args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='upbit_api_request',
    default_args=args,
    concurrency=1,
    max_active_runs=1,
    start_date=datetime(2023,1,31),
    schedule_interval='*/10 * * * *',
    tags=['upbit'],
)   as dag:
    
    def request(**kwargs):
        scr = Screening()
        scr.findSignal()

    tast_upbit_request = PythonOperator(
        task_id=f"upbit_api_request",
        python_callable=request,
        dag=dag,
        provide_context=True,
    )
    tast_upbit_request