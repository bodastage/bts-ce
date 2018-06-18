import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData, HuaweiCM, EricssonCM

huawei_cm = HuaweiCM()
ericsson_cm = EricssonCM()


def extract_network_externals(parent_dag_name, child_dag_name, start_date, schedule_interval):
    """
    Live network external definitions for all vendors

    :param parent_dag_name:
    :param child_dag_name:
    :param start_date:
    :param schedule_interval:
    :return:
    """
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    branch_externals_task = DummyOperator( task_id='branch_externals', dag=dag)
    join_externals_task = DummyOperator(task_id='join_externals', dag=dag)

    def extract_external_definitions_on_ericsson(self):
        ericsson_cm.extract_live_network_externals_on_2g()
        ericsson_cm.extract_live_network_externals_on_3g()
        ericsson_cm.extract_live_network_externals_on_4g()

    extract_external_definitions_on_ericsson_task = BranchPythonOperator(
        task_id='extract_external_definitions_on_ericsson',
        python_callable=extract_external_definitions_on_ericsson,
        dag=dag)

    def extract_external_definitions_on_huawei(self):
        huawei_cm.extract_live_network_externals_on_2g()
        huawei_cm.extract_live_network_externals_on_3g()
        huawei_cm.extract_live_network_externals_on_4g()

    extract_external_definitions_on_huawei_task = BranchPythonOperator(
        task_id='extract_external_definitions_on_huawei',
        python_callable=extract_external_definitions_on_huawei,
        dag=dag)


    dag.set_dependency('branch_externals', 'extract_external_definitions_on_ericsson')
    dag.set_dependency('branch_externals', 'extract_external_definitions_on_huawei')

    dag.set_dependency('extract_external_definitions_on_ericsson', 'join_externals')
    dag.set_dependency('extract_external_definitions_on_huawei', 'join_externals')


    return dag