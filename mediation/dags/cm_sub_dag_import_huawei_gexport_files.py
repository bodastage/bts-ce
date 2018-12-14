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


def import_huawei_gexport_parsed_csv(parent_dag_name, child_dag_name, start_date, schedule_interval):
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

    t23 = DummyOperator( task_id='branch_huawei_4g_importer', dag=dag)

    import_mml_csv = BashOperator(
        task_id='import_huawei_4g_mml_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py huawei_mml_lte /mediation/data/cm/huawei/parsed/mml_lte ',
        dag=dag)

    import_nbi_csv = BashOperator(
        task_id='import_huawei_4g_nbi_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py huawei_nbi_lte /mediation/data/cm/huawei/parsed/nbi_lte ',
        dag=dag)

    import_nbi_csv = BashOperator(
        task_id='import_huawei_4g_gexport_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py huawei_gexport_lte /mediation/data/cm/huawei/parsed/gexport_lte ',
        dag=dag)

    t_join = DummyOperator(
    task_id='join_huawei_4g_importer',
    dag=dag,
    )

    dag.set_dependency('branch_huawei_4g_importer', 'import_huawei_4g_mml_data')
    dag.set_dependency('branch_huawei_4g_importer', 'import_huawei_4g_nbi_data')
    dag.set_dependency('branch_huawei_4g_importer', 'import_huawei_4g_gexport_data')

    dag.set_dependency('import_huawei_4g_mml_data', 'join_huawei_4g_importer')
    dag.set_dependency('import_huawei_4g_nbi_data', 'join_huawei_4g_importer')
    dag.set_dependency('import_huawei_4g_gexport_data', 'join_huawei_4g_importer')


    return dag