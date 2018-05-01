import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from cm_sub_dag_parse_huawei_4g_files import run_huawei_4g_parser
from cm_sub_dag_import_huawei_4g_files import import_huawei_4g_parsed_csv

sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData;

bts_utils = Utils();


def parse_and_import_huawei_4g(parent_dag_name, child_dag_name, start_date, schedule_interval):

    dag_id = '%s.%s' % (parent_dag_name, child_dag_name)

    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    sub_dag_parser_huawei_4g_cm_files = SubDagOperator(
        subdag=run_huawei_4g_parser(dag_id, 'parse_huawei_4g_cm_files', start_date=dag.start_date,
                                    schedule_interval=dag.schedule_interval),
        task_id='parse_huawei_4g_cm_files',
        dag=dag,
    )


    # Clear 4G CM data tables
    def clear_huawei_4g_cm_tables():
        pass

    t50 = PythonOperator(
        task_id='clear_huawei_4g_cm_tables',
        python_callable=clear_huawei_4g_cm_tables,
        dag=dag)

    sub_dag_import_huawei_4g_csv  = SubDagOperator(
        subdag=import_huawei_4g_parsed_csv(dag_id, 'import_huawei_4g_parsed_csv', start_date=dag.start_date,
                                    schedule_interval=dag.schedule_interval),
        task_id='import_huawei_4g_parsed_csv',
        dag=dag,
    )

    dag.set_dependency('parse_huawei_4g_cm_files', 'clear_huawei_4g_cm_tables')
    dag.set_dependency('clear_huawei_4g_cm_tables', 'import_huawei_4g_parsed_csv')

    return dag