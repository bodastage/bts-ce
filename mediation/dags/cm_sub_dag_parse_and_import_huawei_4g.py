import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

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

    t47 = BashOperator(
        task_id='check_if_hua_4g_raw_files_exist',
        bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/huawei/4g/raw/in | wc -l` ]; then exit 1; fi',
        dag=dag)

    t48 = BashOperator(
        task_id='backup_huawei_4g_csv_files',
        bash_command='mv -f /mediation/data/cm/huawei/4g/parsed/in/* /mediation/data/cm/huawei/4g/parsed/out/ 2>/dev/null || true',
        dag=dag)

    t49 = BashOperator(
        task_id='run_huawei_4g_parser',
        bash_command='java -jar /mediation/bin/boda-huaweinbixmlparser.jar /mediation/data/cm/huawei/4g/raw/in /mediation/data/cm/huawei/4g/parsed/in /mediation/conf/cm/hua_cm_4g_nbi_parser.cfg',
        dag=dag)

    # Clear 4G CM data tables
    def clear_huawei_4g_cm_tables():
        pass

    t50 = PythonOperator(
        task_id='clear_huawei_4g_cm_tables',
        python_callable=clear_huawei_4g_cm_tables,
        dag=dag)

    t51 = BashOperator(
        task_id='import_huawei_4g_cm_data',
        bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/hua_cm_4g_nbi_loader.cfg"',
        dag=dag)

    dag.set_dependency('check_if_hua_4g_raw_files_exist', 'backup_huawei_4g_csv_files')
    dag.set_dependency('backup_huawei_4g_csv_files', 'run_huawei_4g_parser')
    dag.set_dependency('run_huawei_4g_parser', 'clear_huawei_4g_cm_tables')
    dag.set_dependency('clear_huawei_4g_cm_tables', 'import_huawei_4g_cm_data')

    return dag