import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData, HuaweiCM, EricssonCM, ZTECM

huawei_cm = HuaweiCM()
ericsson_cm = EricssonCM()
zte_cm = ZTECM()


def run_house_keeping_tasks(parent_dag_name, child_dag_name, start_date, schedule_interval):
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

    start_house_keeping_tasks = DummyOperator( task_id='start_house_keeping', dag=dag)
    join_externals_task = DummyOperator(task_id='end_house_keeping', dag=dag)

    backup_huawei_cm_dumps_task = BashOperator(
        task_id='backup_huawei_cm_dumps',
        bash_command='mv -f /mediation/data/cm/huawei/raw/{nbi,mml,gexport,cfgsyn}/* /mediation/data/cm/huawei/raw/backup/ 2>/dev/null || true',
        dag=dag)


    backup_ericsson_cm_dumps_task = BashOperator(
        task_id='backup_ericsson_cm_dumps',
        bash_command='mv -f /mediation/data/cm/ericsson/raw/{bulkcm,eaw,cnaiv2}/* /mediation/data/cm/ericsson/raw/backup/ 2>/dev/null || true',
        dag=dag)

    backup_zte_cm_dumps_task = BashOperator(
        task_id='backup_zte_cm_dumps',
        bash_command='mv -f /mediation/data/cm/zte/raw/{bulkcm,excel}/* /mediation/data/cm/zte/raw/backup/ 2>/dev/null || true',
        dag=dag)

    backup_nokia_cm_dumps_task = BashOperator(
        task_id='backup_nokia_cm_dumps',
        bash_command='mv -f /mediation/data/cm/nokia/raw/{raml20}/* /mediation/data/cm/nokia/raw/backup/ 2>/dev/null || true',
        dag=dag)


    dag.set_dependency('start_house_keeping', 'backup_huawei_cm_dumps')
    dag.set_dependency('start_house_keeping', 'backup_ericsson_cm_dumps')
    dag.set_dependency('start_house_keeping', 'backup_zte_cm_dumps')
    dag.set_dependency('start_house_keeping', 'backup_nokia_cm_dumps')


    dag.set_dependency('backup_huawei_cm_dumps', 'end_house_keeping')
    dag.set_dependency('backup_ericsson_cm_dumps', 'end_house_keeping')
    dag.set_dependency('backup_zte_cm_dumps', 'end_house_keeping')
    dag.set_dependency('backup_nokia_cm_dumps', 'end_house_keeping')


    return dag