import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData;

bts_utils = Utils();


def parser_and_import_eri_2g(parent_dag_name, child_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    t22 = BashOperator(
        task_id='check_if_2g_raw_files_exist',
        bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/ericsson/2g/raw/in | wc -l` ]; then exit 1; fi',
        dag=dag)

    t21 = BashOperator(
        task_id='backup_ericsson_2g_csv_files',
        bash_command='mv -f /mediation/data/cm/ericsson/2g/parsed/in/* /mediation/data/cm/ericsson/2g/parsed/out/ 2>/dev/null',
        dag=dag)

    t20 = BashOperator(
        task_id='run_ericsson_2g_parser',
        bash_command='java -jar /mediation/bin/boda-ericssoncnaiparser.jar /mediation/data/cm/ericsson/2g/raw/in /mediation/data/cm/ericsson/2g/parsed/in /mediation/conf/cm/eri_cm_2g_cnaiv2_parser.cfg',
        dag=dag)

    def clear_ericsson_2g_cm_tables():
        pass

    t19 = PythonOperator(
        task_id='clear_ericsson_2g_cm_tables',
        python_callable=clear_ericsson_2g_cm_tables,
        dag=dag)

    t18 = BashOperator(
        task_id='import_eri_2g_cm_data',
        bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/eri_cm_2g_loader.cfg"',
        dag=dag)

    dag.set_dependency('check_if_2g_raw_files_exist', 'backup_ericsson_2g_csv_files')
    dag.set_dependency('backup_ericsson_2g_csv_files', 'run_ericsson_2g_parser')
    dag.set_dependency('run_ericsson_2g_parser', 'clear_ericsson_2g_cm_tables')
    dag.set_dependency('clear_ericsson_2g_cm_tables', 'import_eri_2g_cm_data')

    return dag