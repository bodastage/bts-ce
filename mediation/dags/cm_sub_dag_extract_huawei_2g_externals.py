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


def extract_2g_externals(parent_dag_name, child_dag_name, start_date, schedule_interval):
    """
    Parse huawei 4g cm files.

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

    branch_start = BranchPythonOperator(
        task_id='branch_huawei_2g_externals',
        python_callable=pre_clean_up,
        dag=dag)

    def extract_huawei_2g_externals():
        huawei_cm.extract_live_network_2g_externals_on_2g()
        huawei_cm.extract_live_network_3g_externals_on_2g()
        huawei_cm.extract_live_network_4g_externals_on_2g()

    t66 = PythonOperator(
        task_id='extract_huawei_2g_externals',
        python_callable=extract_huawei_2g_externals,
        dag=dag)


    t29 = BashOperator(
        task_id='run_huawei_4g_xml_nbi_parser',
        bash_command='java -jar /mediation/bin/boda-huaweinbixmlparser.jar /mediation/data/cm/huawei/raw/nbi_lte /mediation/data/cm/huawei/parsed/nbi_lte /mediation/conf/cm/huawei_nbi_lte_parser.cfg',
        dag=dag)

    t29_2 = BashOperator(
        task_id='run_huawei_4g_mml_parser',
        bash_command='java -jar /mediation/bin/boda-huaweimmlparser.jar /mediation/data/cm/huawei/raw/mml_lte /mediation/data/cm/huawei/parsed/mml_lte /mediation/conf/cm/huawei_mml_lte_parser.cfg',
        dag=dag)

    run_huawei_2g_xml_gexport_parser = BashOperator(
      task_id='run_huawei_4g_xml_gexport_parser',
      bash_command='java -jar /mediation/bin/boda-huaweicmobjectparser.jar /mediation/data/cm/huawei/raw/gexport_lte /mediation/data/cm/huawei/parsed/gexport_lte /mediation/conf/cm/huawei_gexport_lte_parser.cfg',
      dag=dag)

    t_join = DummyOperator(
        task_id='join_huawei_2g_externals',
        dag=dag,
    )

    dag.set_dependency('branch_huawei_4g_parser', 'run_huawei_4g_mml_parser')
    dag.set_dependency('branch_huawei_4g_parser', 'run_huawei_4g_xml_nbi_parser')
    dag.set_dependency('branch_huawei_4g_parser', 'run_huawei_4g_xml_gexport_parser')

    dag.set_dependency('run_huawei_4g_mml_parser', 'join_huawei_4g_parser')
    dag.set_dependency('run_huawei_4g_xml_nbi_parser', 'join_huawei_4g_parser')
    dag.set_dependency('run_huawei_4g_xml_gexport_parser', 'join_huawei_4g_parser')


    return dag