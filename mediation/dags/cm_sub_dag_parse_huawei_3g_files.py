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


def run_huawei_3g_parser(parent_dag_name, child_dag_name, start_date, schedule_interval):
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
        return 'run_huawei_3g_mml_parser'

    t23 = DummyOperator(
        task_id='branch_huawei_3g_parser',
        dag=dag)

    t29 = BashOperator(
        task_id='run_huawei_3g_xml_nbi_parser',
        bash_command='java -jar /mediation/bin/boda-huaweinbixmlparser.jar /mediation/data/cm/huawei/raw/nbi_umts /mediation/data/cm/huawei/parsed/nbi_umts /mediation/conf/cm/huawei_nbi_umts_parser.cfg',
        dag=dag)

    t29_2 = BashOperator(
        task_id='run_huawei_3g_mml_parser',
        bash_command='java -jar /mediation/bin/boda-huaweimmlparser.jar /mediation/data/cm/huawei/raw/mml_umts /mediation/data/cm/huawei/parsed/mml_umts /mediation/conf/cm/huawei_mml_umts_parser.cfg',
        dag=dag)

    run_huawei_2g_xml_gexport_parser = BashOperator(
      task_id='run_huawei_3g_xml_gexport_parser',
      bash_command='java -jar /mediation/bin/boda-huaweicmobjectparser.jar /mediation/data/cm/huawei/raw/gexport_wcdma /mediation/data/cm/huawei/parsed/gexport_wcdma /mediation/conf/cm/huawei_gexport_wcdma_parser.cfg',
      dag=dag)

    t_join = DummyOperator(
        task_id='join_huawei_3g_parser',
        dag=dag,
    )

    dag.set_dependency('branch_huawei_3g_parser', 'run_huawei_3g_mml_parser')
    dag.set_dependency('branch_huawei_3g_parser', 'run_huawei_3g_xml_nbi_parser')
    dag.set_dependency('branch_huawei_3g_parser', 'run_huawei_3g_xml_gexport_parser')

    dag.set_dependency('run_huawei_3g_mml_parser', 'join_huawei_3g_parser')
    dag.set_dependency('run_huawei_3g_xml_nbi_parser', 'join_huawei_3g_parser')
    dag.set_dependency('run_huawei_3g_xml_gexport_parser', 'join_huawei_3g_parser')


    return dag