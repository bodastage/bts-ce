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


def parse_and_import_eri_3g4g(parent_dag_name, child_dag_name, start_date, schedule_interval):
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    t1 = BashOperator(
        task_id='check_if_ericsson_bulkcm_raw_files_exist',
        # bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/ericsson/3g4g/raw/in | wc -l` ]; then exit 1; fi',
        bash_command='echo "0"',
        dag=dag)

    # Backup previously generate csv files from parsing
    t5 = BashOperator(
        task_id='backup_ericsson_bulkcm_csv_files',
        bash_command='mv -f /mediation/data/cm/ericsson/parsed/bulkcm/* /mediation/data/cm/ericsson/parsed/backup/ 2>/dev/null || true',
        dag=dag)

    t2 = BashOperator(
        task_id='run_ericsson_bulkcm_parser',
        bash_command='java -jar /mediation/bin/boda-bulkcmparser.jar -i /mediation/data/cm/ericsson/raw/bulkcm -o /mediation/data/cm/ericsson/parsed/bulkcm -c /mediation/conf/cm/ericsson_bulkcm_parser.cfg',
        dag=dag)

    # Truncate ericsson 3g4g cm tables
    def clear_ericsson_bulkcm_tables():
        bts_utils.truncate_schema_tables(schema="eri_cm_3g4g")

    t7 = PythonOperator(
        task_id='clear_ericsson_bulkcm_tables',
        python_callable=clear_ericsson_bulkcm_tables,
        dag=dag)


    import_csv_files = BashOperator(
        task_id='import_ericsson_bulkcm_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py ericsson_bulkcm /mediation/data/cm/ericsson/parsed/bulkcm ',
        dag=dag)

    dag.set_dependency('check_if_ericsson_bulkcm_raw_files_exist', 'backup_ericsson_bulkcm_csv_files')
    dag.set_dependency('backup_ericsson_bulkcm_csv_files', 'run_ericsson_bulkcm_parser')
    dag.set_dependency('run_ericsson_bulkcm_parser', 'clear_ericsson_bulkcm_tables')
    dag.set_dependency('clear_ericsson_bulkcm_tables', 'import_ericsson_bulkcm_data')

    return dag

