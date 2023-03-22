import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import pprint
import pandas as pd
from datetime import datetime, timedelta
from lib.Mysql import Mysql
from lib.Mongo import Mongo
from lib.config import MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE


dag=DAG(
    dag_id="week_time_mysql_load",
    start_date=datetime(2023, 2, 1),
    schedule_interval="10 15 * * 0",
    catchup=True,
)

target_info_table_list = [
        ("WEEK_DEAD_CROSS_INFO", "week_dead_cross_time"),
        ("WEEK_GOLDEN_CROSS_INFO", "week_golden_cross_time"),
        ("WEEK_PUMPING_INFO", "week_pumping_time"),
        ("WEEK_VOL_INFO", "week_vol_time"),
    ]

target_time_dtl_table_list = [
        ("WEEK_DEAD_CROSS_TIME_DTL", "week_dead_cross_time"),
        ("WEEK_GOLDEN_CROSS_TIME_DTL", "week_golden_cross_time"),
        ("WEEK_PUMPING_TIME_DTL", "week_pumping_time"),
        ("WEEK_VOL_TIME_DTL", "week_vol_time"),
    ]

target_day_dtl_table_list = [
        ("WEEK_DEAD_CROSS_DAY_DTL", "week_dead_cross_time"),
        ("WEEK_GOLDEN_CROSS_DAY_DTL", "week_golden_cross_time"),
        ("WEEK_PUMPING_DAY_DTL", "week_pumping_time"),
        ("WEEK_VOL_DAY_DTL", "week_vol_time"),
    ]

source_table_header_list = [
        "WEEK_DEAD_CROSS",
        "WEEK_GOLDEN_CROSS",
        "WEEK_PUMPING",
        "WEEK_VOL",
    ]


def _conn_mysql():
    mysql = Mysql(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE)
    mysql.connect()
    return mysql

def _load_info_to_mysql(**kwargs):
    value_params = Mongo("coins").find_week_source_col(kwargs["target_info_table"][1], kwargs['data_interval_start'] + timedelta(hours=9))
    mysql = _conn_mysql()
    mysql.load_week_info(kwargs["target_info_table"][0], value_params)

def _load_time_dtl_to_mysql(**kwargs):
    def _make_value_params(data):
        df = pd.DataFrame(data)
        df = df.explode("signalTimeList").rename(columns={"signalTimeList": "signalTime"})
        df = df.drop(columns=["signalDayList"], inplace=False)
        return df.to_dict("records")

    value_params = Mongo("coins").find_week_source_col(kwargs["target_dtl_table"][1], kwargs['data_interval_start'] + timedelta(hours=9))
    value_params = _make_value_params(value_params)
    mysql = _conn_mysql()
    mysql.load_week_time_dtl(kwargs["target_dtl_table"][0], value_params)

def _load_day_dtl_to_mysql(**kwargs):
    def _make_value_params(data):
        df = pd.DataFrame(data)
        df = df.explode("signalDayList").rename(columns={"signalDayList": "signalDay"})
        df = df.drop(columns=["signalTimeList"], inplace=False)
        return df.to_dict("records")

    value_params = Mongo("coins").find_week_source_col(kwargs["target_dtl_table"][1], kwargs['data_interval_start'] + timedelta(hours=9))
    value_params = _make_value_params(value_params)
    mysql = _conn_mysql()
    mysql.load_week_day_dtl(kwargs["target_dtl_table"][0], value_params)

def _load_week_time(**kwargs):
    mysql = _conn_mysql()
    mysql.load_week_time(kwargs["source_table_header"], kwargs['data_interval_start'] + timedelta(hours=9))

def _load_week_day(**kwargs):
    mysql = _conn_mysql()
    mysql.load_week_day(kwargs["source_table_header"], kwargs['data_interval_start'] + timedelta(hours=9))

dummy_1 = DummyOperator(
    task_id = "dummy_1",
)
dummy_2 = DummyOperator(
    task_id = "dummy_2",
)

for target_info_table in target_info_table_list:
    load_info_to_mysql = PythonOperator(
        task_id = f"load_info_to_mysql_{target_info_table[0]}",
        python_callable = _load_info_to_mysql,
        op_kwargs = {"target_info_table": target_info_table},
        dag = dag,
    )
    load_info_to_mysql >> dummy_1
for target_dtl_table in target_time_dtl_table_list:
    load_time_dtl_to_mysql = PythonOperator(
        task_id = f"load_time_dtl_to_mysql_{target_dtl_table[0]}",
        python_callable = _load_time_dtl_to_mysql,
        op_kwargs = {"target_dtl_table": target_dtl_table},
        dag = dag,
    )
    dummy_1 >> load_time_dtl_to_mysql >> dummy_2
for target_dtl_table in target_day_dtl_table_list:
    load_day_dtl_to_mysql = PythonOperator(
        task_id = f"load_day_dtl_to_mysql_{target_dtl_table[0]}",
        python_callable = _load_day_dtl_to_mysql,
        op_kwargs = {"target_dtl_table": target_dtl_table},
        dag = dag,
    )
    dummy_1 >> load_day_dtl_to_mysql >> dummy_2
for source_table_header in source_table_header_list:
    load_week_time = PythonOperator(
        task_id = f"load_week_time_{source_table_header}",
        python_callable = _load_week_time,
        op_kwargs = {"source_table_header": source_table_header},
        dag = dag,
    )
    dummy_2 >> load_week_time
for source_table_header in source_table_header_list:
    load_week_day = PythonOperator(
        task_id = f"load_week_day_{source_table_header}",
        python_callable = _load_week_day,
        op_kwargs = {"source_table_header": source_table_header},
        dag = dag,
    )
    dummy_2 >> load_week_day