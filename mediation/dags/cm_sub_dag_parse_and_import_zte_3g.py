import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from cm_sub_dag_parse_huawei_2g_files import run_huawei_2g_parser
from cm_sub_dag_import_huawei_2g_files import import_huawei_2g_parsed_csv

sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData;

bts_utils = Utils();


def parse_and_import_zte_3g(parent_dag_name, child_dag_name, start_date, schedule_interval):
    """Parse and import ZTE 2G"""

    dag_id = '%s.%s' % (parent_dag_name, child_dag_name)
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    # @TODO: Investigate other ways to check if there are not files yet
    t28 = BashOperator(
        task_id='check_if_zte_3g_raw_files_exist',
        bash_command='ls -1 /mediation/data/cm/zte/raw/in | wc -l',
        dag=dag)

    # @TODO: Backup parsed files
    t30 = BashOperator(
        task_id='backup_zte_3g_csv_files',
        bash_command='mv -f /mediation/data/cm/zte/3g/parsed/bulkcm_umts/* /mediation/data/cm/zte/parsed/backup/ 2>/dev/null || true',
        dag=dag)

    def clear_zte_2g_cm_tables():
        pass

    t31 = PythonOperator(
        task_id='clear_zte_3g_cm_tables',
        python_callable=clear_zte_2g_cm_tables,
        dag=dag)

    parse_zte_2g_cm_files = BashOperator(
      task_id='parse_zte_3g_cm_files',
      bash_command='java -jar /mediation/bin/boda_bulkcmparser.jar /mediation/data/cm/zte/raw/bulkcm_umts /mediation/data/cm/zte/parsed/bulkcm_umts /mediation/conf/cm/zte_cm_3g_blkcm_parser.cfg',
      dag=dag)

    import_zte_cm_data = BashOperator(
        task_id='import_zte_3g_cm_data',
        bash_command='python /mediation/bin/load_cm_data_into_db.py zte_bulkcm_umts /mediation/data/cm/zte/parsed/bulkcm_umts ',
        dag=dag)

    dag.set_dependency('check_if_zte_3g_raw_files_exist', 'backup_zte_3g_csv_files')
    dag.set_dependency('backup_zte_3g_csv_files', 'parse_zte_3g_cm_files')
    dag.set_dependency('parse_zte_3g_cm_files', 'clear_zte_3g_cm_tables')
    dag.set_dependency('clear_zte_3g_cm_tables', 'import_zte_3g_cm_data')

    return dag