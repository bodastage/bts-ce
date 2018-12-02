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


def import_huawei_2g_parsed_csv(parent_dag_name, child_dag_name, start_date, schedule_interval):
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

    t23 = DummyOperator( task_id='branch_huawei_2g_importer', dag=dag)

    import_mml_csv = BashOperator(
        task_id='import_huawei_2g_mml_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py huawei_mml_gsm /mediation/data/cm/huawei/parsed/mml_gsm ',
        dag=dag)

    import_nbi_csv = BashOperator(
        task_id='import_huawei_2g_nbi_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py huawei_nbi_gsm /mediation/data/cm/huawei/parsed/nbi_gsm ',
        dag=dag)

    import_nbi_csv = BashOperator(
        task_id='import_huawei_2g_gexport_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py huawei_gexport_gsm /mediation/data/cm/huawei/parsed/gexport_gsm ',
        dag=dag)

    t_join = DummyOperator(
    task_id='join_huawei_2g_importer',
    dag=dag,
    )

    t_run_huawei_gexport_gsm_insert_queries = BashOperator(
        task_id='run_huawei_gexport_gsm_insert_queries',
        bash_command='python /mediation/bin/run_cm_load_insert_queries.py huawei_gexport_gsm',
        dag=dag)


    dag.set_dependency('branch_huawei_2g_importer', 'import_huawei_2g_mml_data')
    dag.set_dependency('branch_huawei_2g_importer', 'import_huawei_2g_nbi_data')
    dag.set_dependency('branch_huawei_2g_importer', 'import_huawei_2g_gexport_data')
    dag.set_dependency('import_huawei_2g_gexport_data', 'run_huawei_gexport_gsm_insert_queries')

    dag.set_dependency('import_huawei_2g_mml_data', 'join_huawei_2g_importer')
    dag.set_dependency('import_huawei_2g_nbi_data', 'join_huawei_2g_importer')
    dag.set_dependency('run_huawei_gexport_gsm_insert_queries', 'join_huawei_2g_importer')


    return dag