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


def run_network_audits(parent_dag_name, child_dag_name, start_date, schedule_interval):
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

    branch_network_audits_task = DummyOperator( task_id='branch_network_audits', dag=dag)
    join_network_audits_task = DummyOperator(task_id='join_network_audits', dag=dag)

    def base_line_audits(self):
        """Run network baseline audits"""
        pass

    base_line_audits = BranchPythonOperator(
        task_id='base_line_audits',
        python_callable=base_line_audits,
        dag=dag)


    dag.set_dependency('branch_network_audits', 'base_line_audits')

    dag.set_dependency('base_line_audits', 'join_network_audits')


    return dag