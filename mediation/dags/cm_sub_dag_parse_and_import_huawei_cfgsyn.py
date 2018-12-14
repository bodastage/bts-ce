import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from cm_sub_dag_parse_huawei_4g_files import run_huawei_4g_parser

sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData;

bts_utils = Utils();


def parse_and_import_huawei_cfgsyn(parent_dag_name, child_dag_name, start_date, schedule_interval):

    dag_id = '%s.%s' % (parent_dag_name, child_dag_name)

    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    parse_huawei_cfgsyn_cm_files = BashOperator(
      task_id='parse_huawei_cfgsyn_cm_files',
      bash_command='java -jar /mediation/bin/boda-huaweicfgsynparser.jar /mediation/data/cm/huawei/raw/cfgsyn /mediation/data/cm/huawei/parsed/cfgsyn /mediation/conf/cm/huawei_cfgsyn_parser.cfg',
      dag=dag)

    import_nbi_csv = BashOperator(
        task_id='import_huawei_cfgsyn_parsed_csv',
        bash_command='python /mediation/bin/load_cm_data_into_db.py huawei_cfgsyn /mediation/data/cm/huawei/parsed/cfgsyn',
        dag=dag)


    # Clear cfgsyn CM data tables
    def clear_huawei_cfgsyn_cm_tables():
        pass

    t50 = PythonOperator(
        task_id='clear_huawei_cfgsyn_cm_tables',
        python_callable=clear_huawei_cfgsyn_cm_tables,
        dag=dag)


    dag.set_dependency('parse_huawei_cfgsyn_cm_files', 'clear_huawei_cfgsyn_cm_tables')
    dag.set_dependency('clear_huawei_cfgsyn_cm_tables', 'import_huawei_cfgsyn_parsed_csv')

    return dag