import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

# sys.path.append('/mediation/packages');
#
# from bts import NetworkBaseLine, Utils, ProcessCMData;
#
# bts_utils = Utils();


def run_huawei_2g_parser(parent_dag_name, child_dag_name, start_date, schedule_interval):
    """
    Parse huawei 2g cm files.

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

    def get_cm_file_format():
      # if  'huawei_mml'
      return 'run_huawei_2g_mml_parser'

    t23 = BranchPythonOperator(
      task_id='branch_huawei_2g_parser',
      python_callable=get_cm_file_format,
      dag=dag)

    t29 = BashOperator(
      task_id='run_huawei_2g_xml_nbi_parser',
      bash_command='java -jar /mediation/bin/boda-huaweinbixmlparser.jar /mediation/data/cm/huawei/2g/raw/in /mediation/data/cm/huawei/2g/parsed/in /mediation/conf/cm/hua_cm_2g_nbi_parameters.cfg',
      dag=dag)

    t29_2 = BashOperator(
      task_id='run_huawei_2g_mml_parser',
      bash_command='java -jar /mediation/bin/boda-huaweimmlparser.jar /mediation/data/cm/huawei/2g/raw/in /mediation/data/cm/huawei/2g/parsed/in /mediation/conf/cm/hua_cm_2g_nbi_parameters.cfg',
      dag=dag)

    t_join = DummyOperator(
    task_id='join_huawei_2g_parser',
    dag=dag,
    )

    dag.set_dependency('branch_huawei_2g_parser', 'run_huawei_2g_mml_parser')
    dag.set_dependency('branch_huawei_2g_parser', 'run_huawei_2g_xml_nbi_parser')

    dag.set_dependency('run_huawei_2g_mml_parser', 'join_huawei_2g_parser')
    dag.set_dependency('run_huawei_2g_xml_nbi_parser', 'join_huawei_2g_parser')


    return dag