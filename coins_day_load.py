import airflow.utils.dates
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from lib.Mongo import Mongo

dag=DAG(
    dag_id="coins_day_load",
    start_date=datetime(2023, 2, 1),
    schedule_interval= "0 15 * * *",
    catchup=True
)

def _load_day_coins(type, **kwargs):
    print(f"data_interval_start = {kwargs['data_interval_start']}")
    if type == "pumping":
        Mongo("coins").load_day_coins("pumping", kwargs['data_interval_start'] + timedelta(hours=9))
    elif type == "vol":
        Mongo("coins").load_day_coins("vol", kwargs['data_interval_start'] + timedelta(hours=9))
    elif type == "cross":
        Mongo("coins").load_day_coins("cross", kwargs['data_interval_start'] + timedelta(hours=9))

def _load_day_golden_cross(type, **kwargs):
    print(f"data_interval_start = {kwargs['data_interval_start']}")
    if type == "not_yet":
        Mongo("coins").load_day_not_yet_golden_cross(kwargs['data_interval_start'] + timedelta(hours=9))
    elif type == "yet":
        Mongo("coins").load_day_yet_golden_cross(kwargs['data_interval_start'] + timedelta(hours=9))

def _load_day_pumping(type, **kwargs):
    print(f"data_interval_start = {kwargs['data_interval_start']}")
    if type == "not_yet":
        Mongo("coins").load_day_pumping_not_yet(kwargs['data_interval_start'] + timedelta(hours=9))
    elif type == "yet":
        Mongo("coins").load_day_pumping_yet(kwargs['data_interval_start'] + timedelta(hours=9))

load_day_pumping = PythonOperator(
    task_id = "load_day_pumping",
    python_callable = _load_day_coins,
    op_kwargs = {"type": "pumping"},
    dag = dag,
)
load_day_vol = PythonOperator(
    task_id = "load_day_vol",
    python_callable = _load_day_coins,
    op_kwargs = {"type": "vol"},
    dag = dag,
)
load_day_cross = PythonOperator(
    task_id = "load_day_cross",
    python_callable = _load_day_coins,
    op_kwargs = {"type": "cross"},
    dag = dag,
)
load_day_not_yet_golden_cross = PythonOperator(
    task_id = "load_day_not_yet_golden_cross",
    python_callable = _load_day_golden_cross,
    op_kwargs = {"type": "not_yet"},
    dag = dag,
)
load_day_yet_golden_cross = PythonOperator(
    task_id = "load_day_yet_golden_cross",
    python_callable = _load_day_golden_cross,
    op_kwargs = {"type": "yet"},
    dag = dag,
)
load_day_not_yet_pumping = PythonOperator(
    task_id = "load_day_not_yet_pumping",
    python_callable = _load_day_pumping,
    op_kwargs = {"type": "not_yet"},
    dag = dag,
)
load_day_yet_pumping = PythonOperator(
    task_id = "load_day_yet_pumping",
    python_callable = _load_day_pumping,
    op_kwargs = {"type": "yet"},
    dag = dag,
)
dummy = DummyOperator(
    task_id = "dummy",
)

load_day_pumping >> dummy >> [load_day_not_yet_pumping, load_day_yet_pumping]
load_day_vol
load_day_cross >> [load_day_not_yet_golden_cross, load_day_yet_golden_cross] >> dummy