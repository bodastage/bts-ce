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


def parse_and_import_eri_2g(parent_dag_name, child_dag_name, start_date, schedule_interval):
    """Parse and import Ericsson GSM CM data"""
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    t22 = BashOperator(
        task_id='check_if_cnaiv2_raw_files_exist',
        # bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/ericsson/2g/raw/in | wc -l` ]; then exit 1; fi',
        bash_command='echo 0;',
        dag=dag)

    # .. || true added to make sure the command alwasy succeeds
    t21 = BashOperator(
        task_id='backup_ericsson_cnaiv2_csv_files',
        bash_command='mv -f /mediation/data/cm/ericsson/parsed/cnaiv2/* /mediation/data/cm/ericsson/parsed/backup/ 2>/dev/null || true',
        dag=dag)

    t20 = BashOperator(
        task_id='run_ericsson_cnaiv2_parser',
        bash_command='java -jar /mediation/bin/boda-ericssoncnaiparser.jar /mediation/data/cm/ericsson/raw/cnaiv2 /mediation/data/cm/ericsson/parsed/cnaiv2 /mediation/conf/cm/ericsson_cnaiv2_gsm_parser.cfg',
        dag=dag)

    def clear_ericsson_cnaiv2_cm_tables():
        pass

    t19 = PythonOperator(
        task_id='clear_ericsson_cnaiv2_cm_tables',
        python_callable=clear_ericsson_cnaiv2_cm_tables,
        dag=dag)


    import_csv_files = BashOperator(
        task_id='import_ericsson_cnaiv2_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py ericsson_cnaiv2 /mediation/data/cm/ericsson/parsed/cnaiv2 ',
        dag=dag)

    dag.set_dependency('check_if_cnaiv2_raw_files_exist', 'backup_ericsson_cnaiv2_csv_files')
    dag.set_dependency('backup_ericsson_cnaiv2_csv_files', 'run_ericsson_cnaiv2_parser')
    dag.set_dependency('run_ericsson_cnaiv2_parser', 'clear_ericsson_cnaiv2_cm_tables')
    dag.set_dependency('clear_ericsson_cnaiv2_cm_tables', 'import_ericsson_cnaiv2_data')

    return dag