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

    t23 = DummyOperator( task_id='branch_huawei_2g_parser', dag=dag)

    t29 = BashOperator(
      task_id='run_huawei_2g_xml_nbi_parser',
      bash_command='java -jar /mediation/bin/boda-huaweinbixmlparser.jar /mediation/data/cm/huawei/raw/nbi_gsm /mediation/data/cm/huawei/parsed/nbi_gsm /mediation/conf/cm/huawei_nbi_gsm_parser.cfg',
      dag=dag)

    t29_2 = BashOperator(
      task_id='run_huawei_2g_mml_parser',
      bash_command='java -jar /mediation/bin/boda-huaweimmlparser.jar /mediation/data/cm/huawei/raw/mml_gsm /mediation/data/cm/huawei/parsed/mml_gsm /mediation/conf/cm/huawei_mml_gsm_parser.cfg',
      dag=dag)

    t29_3 = BashOperator(
      task_id='run_huawei_2g_xml_gexport_parser',
      bash_command='java -jar /mediation/bin/boda-huaweicmobjectparser.jar /mediation/data/cm/huawei/raw/gexport_gsm /mediation/data/cm/huawei/parsed/gexport_gsm /mediation/conf/cm/huawei_gexport_gsm_parser.cfg',
      dag=dag)

    t_join = DummyOperator(
    task_id='join_huawei_2g_parser',
    dag=dag,
    )

    t_strip_neversion_from_mo = BashOperator(
        task_id='strip_neversion_from_gsm_mos',
        bash_command='sed -i -r " s/_(BSC6900GSM|BSC6900UMTS|BSC6900GU|BSC6910GSM|BSC6910UMTS|BSC6910GU)//ig; s/_(BTS3900|PICOBTS3900|BTS3911B|PICOBTS3911B|MICROBTS3900|MICROBTS3911B)//ig;" /mediation/data/cm/huawei/raw/gexport_gsm/*',
        dag=dag)

    dag.set_dependency('branch_huawei_2g_parser', 'run_huawei_2g_mml_parser')
    dag.set_dependency('branch_huawei_2g_parser', 'run_huawei_2g_xml_nbi_parser')
    dag.set_dependency('branch_huawei_2g_parser', 'strip_neversion_from_gsm_mos')
    dag.set_dependency('strip_neversion_from_gsm_mos', 'run_huawei_2g_xml_gexport_parser')

    dag.set_dependency('run_huawei_2g_mml_parser', 'join_huawei_2g_parser')
    dag.set_dependency('run_huawei_2g_xml_nbi_parser', 'join_huawei_2g_parser')
    dag.set_dependency('run_huawei_2g_xml_gexport_parser', 'join_huawei_2g_parser')


    return dag