import  sys
import os
import airflow
from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from datetime import timedelta
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from airflow.utils.trigger_rule import TriggerRule
from cm_sub_dag_extract_externals import extract_network_externals
from airflow.sensors.external_task_sensor import ExternalTaskSensor

sys.path.append('/mediation/packages')

from bts import NetworkBaseLine, Utils

schedule_interval = "@daily" # # bts_utils.get_setting('cm_dag_schedule_interval')

args = {
    'owner': 'bodastage',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'email': ['support@bodastage.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


dag = DAG(
    dag_id='network_baseline',
    default_args=args,
    schedule_interval=schedule_interval,
    start_date=datetime(2017, 1, 1),
    max_active_runs = 1,
    # concurrency = 1,
    catchup = False,
    dagrun_timeout=timedelta(minutes=24*60)) # dag runs out after 1 day of running

nb  = NetworkBaseLine()


def init_network_baseline():
    nb.delete_counts()


start_baseline = PythonOperator(
    task_id='start_baseline',
    python_callable=init_network_baseline,
    dag=dag)


DummyOperator(task_id='end_baseline', dag=dag)


def compute_huawei_2g_value_counts():
    nb.compute_huawei_2g3g_value_counts('2G')


huawei_2g_value_counts = PythonOperator(
    task_id='huawei_2g_value_counts',
    python_callable=compute_huawei_2g_value_counts,
    dag=dag)


def compute_huawei_3g_value_counts():
    nb.compute_huawei_2g3g_value_counts('3G')


huawei_3g_value_counts = PythonOperator(
    task_id='huawei_3g_value_counts',
    python_callable=compute_huawei_3g_value_counts,
    dag=dag)


def compute_huawei_4g_value_counts():
    nb.compute_huawei_4g_value_counts()


huawei_4g_value_counts = PythonOperator(
    task_id='huawei_4g_value_counts',
    python_callable=compute_huawei_4g_value_counts,
    dag=dag)


def compute_ericsson_2g_value_counts():
    nb.compute_ericsson_2g_value_counts()


ericsson_2g_value_counts = PythonOperator(
    task_id='ericsson_2g_value_counts',
    python_callable=compute_ericsson_2g_value_counts,
    dag=dag)


def compute_ericsson_3g_value_counts():
    nb.compute_ericsson_3g_value_counts()


ericsson_3g_value_counts = PythonOperator(
    task_id='ericsson_3g_value_counts',
    python_callable=compute_ericsson_3g_value_counts,
    dag=dag)


def compute_ericsson_4g_value_counts():
    nb.compute_ericsson_4g_value_counts()


ericsson_4g_value_counts = PythonOperator(
    task_id='ericsson_4g_value_counts',
    python_callable=compute_ericsson_4g_value_counts,
    dag=dag)


def compute_zte_2g_value_counts():
    nb.compute_zte_2g_value_counts()


zte_2g_value_counts = PythonOperator(
    task_id='zte_2g_value_counts',
    python_callable=compute_zte_2g_value_counts,
    dag=dag)


def compute_zte_3g_value_counts():
    nb.compute_zte_3g_value_counts()


zte_3g_value_counts = PythonOperator(
    task_id='zte_3g_value_counts',
    python_callable=compute_zte_3g_value_counts,
    dag=dag)


def compute_zte_4g_value_counts():
    nb.compute_zte_4g_value_counts()


zte_4g_value_counts = PythonOperator(
    task_id='zte_4g_value_counts',
    python_callable=compute_zte_4g_value_counts,
    dag=dag)


def compute_network_baseline():
    """Populates baseline.network_baseline with the network baseline values"""
    nb.run_network_baseline()


compute_baseline = PythonOperator(
    task_id='compute_network_baseline',
    python_callable=compute_network_baseline,
    dag=dag)

def network_baseline_audit():
    nb.run_baseline_audit()


run_network_baseline_audit = PythonOperator(
    task_id='run_network_baseline_audit',
    python_callable=compute_network_baseline,
    dag=dag)



ext_dep = ExternalTaskSensor(
    external_dag_id='cm_load',
    external_task_id='end_cm_load',
    task_id='start_baseline',
    dag=dag)




dag.set_dependency('start_baseline', 'huawei_2g_value_counts')
dag.set_dependency('start_baseline', 'huawei_3g_value_counts')
dag.set_dependency('start_baseline', 'huawei_4g_value_counts')

dag.set_dependency('start_baseline', 'ericsson_2g_value_counts')
dag.set_dependency('start_baseline', 'ericsson_3g_value_counts')
dag.set_dependency('start_baseline', 'ericsson_4g_value_counts')

dag.set_dependency('start_baseline', 'zte_2g_value_counts')
dag.set_dependency('start_baseline', 'zte_3g_value_counts')
dag.set_dependency('start_baseline', 'zte_4g_value_counts')

dag.set_dependency('huawei_2g_value_counts', 'compute_network_baseline')
dag.set_dependency('huawei_3g_value_counts', 'compute_network_baseline')
dag.set_dependency('huawei_4g_value_counts', 'compute_network_baseline')

dag.set_dependency('ericsson_2g_value_counts', 'compute_network_baseline')
dag.set_dependency('ericsson_3g_value_counts', 'compute_network_baseline')
dag.set_dependency('ericsson_4g_value_counts', 'compute_network_baseline')

dag.set_dependency('zte_2g_value_counts', 'compute_network_baseline')
dag.set_dependency('zte_3g_value_counts', 'compute_network_baseline')
dag.set_dependency('zte_4g_value_counts', 'compute_network_baseline')


dag.set_dependency('compute_network_baseline', 'end_baseline')


