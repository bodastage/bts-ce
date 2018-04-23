import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from cm_sub_dag_parse_huawei_3g_files import run_huawei_3g_parser
from cm_sub_dag_import_huawei_3g_files import import_huawei_3g_parsed_csv

sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData;

bts_utils = Utils();


def parse_and_import_huawei_3g(parent_dag_name, child_dag_name, start_date, schedule_interval):

    dag_id = '%s.%s' % (parent_dag_name, child_dag_name)

    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    # @TODO: Investigate other ways to check if there are not files yet considering all the formats supported
    t42 = BashOperator(
        task_id='check_if_hua_3g_raw_files_exist',
        # bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/huawei/3g/raw/in | wc -l` ]; then exit 1; fi',
        bash_command='ls -1 /mediation/data/cm/huawei/2g/raw/in | wc -l',
        dag=dag)

    t43 = BashOperator(
        task_id='backup_huawei_3g_csv_files',
        bash_command='mv -f /mediation/data/cm/huawei/3g/parsed/in/* /mediation/data/cm/huawei/3g/parsed/out/ 2>/dev/null || true',
        dag=dag)

    # Clear 3G CM data tables
    def clear_huawei_3g_cm_tables():
        pass

    t45 = PythonOperator(
        task_id='clear_huawei_3g_cm_tables',
        python_callable=clear_huawei_3g_cm_tables,
        dag=dag)

    # t46 = BashOperator(
    #     task_id='import_huawei_3g_cm_data',
    #     bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/hua_cm_3g_nbi_loader.cfg"',
    #     dag=dag)

    sub_dag_import_huawei_3g_csv  = SubDagOperator(
        subdag=import_huawei_3g_parsed_csv(dag_id, 'import_huawei_3g_parsed_csv', start_date=dag.start_date,
                                    schedule_interval=dag.schedule_interval),
        task_id='import_huawei_3g_parsed_csv',
        dag=dag,
    )

    sub_dag_parser_huawei_3g_cm_files = SubDagOperator(
        subdag=run_huawei_3g_parser(dag_id, 'parse_huawei_3g_cm_files', start_date=dag.start_date,
                                    schedule_interval=dag.schedule_interval),
        task_id='parse_huawei_3g_cm_files',
        dag=dag,
    )

    dag.set_dependency('check_if_hua_3g_raw_files_exist', 'backup_huawei_3g_csv_files')
    dag.set_dependency('backup_huawei_3g_csv_files', 'parse_huawei_3g_cm_files')
    dag.set_dependency('parse_huawei_3g_cm_files', 'clear_huawei_3g_cm_tables')
    dag.set_dependency('clear_huawei_3g_cm_tables', 'import_huawei_3g_parsed_csv')

    return dag