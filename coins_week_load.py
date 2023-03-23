import airflow.utils.dates
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.Mongo import Mongo

dag=DAG(
    dag_id="coins_week_load",
    start_date=datetime(2023, 2, 1),
    schedule_interval="5 15 * * 0",
    catchup=True
)

def _load_week_coins(type, **kwargs):
    print(f"data_interval_start = {kwargs['data_interval_start']}")
    if type == "pumping":
        Mongo("coins").load_week_pumping_time(kwargs['data_interval_start'] + timedelta(hours=9))
    elif type == "vol":
        Mongo("coins").load_week_vol_time(kwargs['data_interval_start'] + timedelta(hours=9))
    elif type == "cross":
        Mongo("coins").load_week_cross_time(kwargs['data_interval_start'] + timedelta(hours=9))

load_week_pumping = PythonOperator(
    task_id = "load_week_pumping",
    python_callable = _load_week_coins,
    op_kwargs = {"type": "pumping"},
    dag = dag,
)
load_week_vol = PythonOperator(
    task_id = "load_week_vol",
    python_callable = _load_week_coins,
    op_kwargs = {"type": "vol"},
    dag = dag,
)
load_week_cross = PythonOperator(
    task_id = "load_week_cross",
    python_callable = _load_week_coins,
    op_kwargs = {"type": "cross"},
    dag = dag,
)

load_week_pumping
load_week_vol
load_week_cross