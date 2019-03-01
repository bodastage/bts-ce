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
from airflow.sensors.external_task_sensor import ExternalTaskSensor

sys.path.append('/mediation/packages')

from bts import NetworkBaseLine, Utils, ProcessCMData, HuaweiCM, EricssonCM, NetworkAudit

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
    dag_id='network_audits',
    default_args=args,
    schedule_interval=schedule_interval,
    start_date=datetime(2017, 1, 1),
    max_active_runs = 1,
    # concurrency = 1,
    catchup = False,
    dagrun_timeout=timedelta(minutes=24*60)) # dag runs out after 1 day of running

DummyOperator(task_id='start_network_audits', dag=dag)
DummyOperator(task_id='end_network_audits', dag=dag)


ext_dep = ExternalTaskSensor(
    external_dag_id='cm_load',
    external_task_id='end_cm_load',
    task_id='start_network_audits',
    dag=dag)


def inconsistent_gsm_externals():
    """
    Generate GSM external inconsistencies
    """
    network_audit = NetworkAudit()
    network_audit.generate_incosistent_gsm_externals()


def inconsistent_umts_externals():
    """
    Generate UMTS external inconsistencies
    """
    network_audit = NetworkAudit()
    network_audit.generate_incosistent_umts_externals()


def inconsistent_lte_externals():
    """
    Generate LTE external inconsistencies
    """
    network_audit = NetworkAudit()
    network_audit.generate_incosistent_lte_externals()


def generate_missing_one_way_relations():
    network_audit = NetworkAudit()
    network_audit.generate_missing_one_way_relations()


def generate_missing_cosite_relations():
    network_audit = NetworkAudit()
    network_audit.generate_missing_cosite_relations()


inconsistent_gsm_externals_task = PythonOperator(
    task_id='generate_inconsistent_gsm_externals',
    python_callable=inconsistent_gsm_externals,
    dag=dag)

inconsistent_umts_externals_task = PythonOperator(
    task_id='generate_inconsistent_umts_externals',
    python_callable=inconsistent_umts_externals,
    dag=dag)

inconsistent_lte_externals_task = PythonOperator(
    task_id='generate_inconsistent_lte_externals',
    python_callable=inconsistent_lte_externals,
    dag=dag)

generate_missing_one_way_relations_task = PythonOperator(
    task_id='generate_missing_one_way_relations',
    python_callable=generate_missing_one_way_relations,
    dag=dag)

generate_missing_cosite_relations_task = PythonOperator(
    task_id='generate_missing_cosite_relations',
    python_callable=generate_missing_cosite_relations,
    dag=dag)

dag.set_dependency('start_network_audits', 'generate_inconsistent_gsm_externals')
dag.set_dependency('start_network_audits', 'generate_inconsistent_umts_externals')
dag.set_dependency('start_network_audits', 'generate_inconsistent_lte_externals')
dag.set_dependency('start_network_audits', 'generate_missing_one_way_relations')
dag.set_dependency('start_network_audits', 'generate_missing_cosite_relations')

dag.set_dependency('generate_inconsistent_gsm_externals', 'end_network_audits')
dag.set_dependency('generate_inconsistent_umts_externals', 'end_network_audits')
dag.set_dependency('generate_inconsistent_lte_externals', 'end_network_audits')
dag.set_dependency('generate_missing_one_way_relations', 'end_network_audits')
dag.set_dependency('generate_missing_cosite_relations', 'end_network_audits')


