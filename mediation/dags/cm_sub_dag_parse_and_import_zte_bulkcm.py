import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from cm_sub_dag_import_huawei_gexport_files import import_huawei_gexport_parsed_csv

sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData;


bts_utils = Utils();


def parse_and_import_zte_bulkcm(parent_dag_name, child_dag_name, start_date, schedule_interval):

    dag_id = '%s.%s' % (parent_dag_name, child_dag_name)

    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    parse_zte_bulkcm_cm_files = BashOperator(
      task_id='parse_zte_bulkcm_cm_files',
      bash_command='java -jar /mediation/bin/boda-bulkcmparser.jar -i /mediation/data/cm/zte/raw/bulkcm -o /mediation/data/cm/zte/parsed/bulkcm -c /mediation/conf/cm/zte_bulkcm_parser.cfg',
      dag=dag)

    import_mml_csv = BashOperator(
        task_id='import_zte_bulkcm_parsed_csv',
        bash_command='python /mediation/bin/load_cm_data_into_db.py zte_bulkcm /mediation/data/cm/zte/parsed/bulkcm',
        dag=dag)

    t_run_zte_bulkcm_insert_queries = BashOperator(
        task_id='run_zte_bulkcm_insert_queries',
        bash_command='python /mediation/bin/run_cm_load_insert_queries.py zte_bulkcm',
        dag=dag)


    # Clear 4G CM data tables
    def clear_zte_bulkcm_cm_tables():
        pass

    t50 = PythonOperator(
        task_id='clear_zte_bulkcm_cm_tables',
        python_callable=clear_zte_bulkcm_cm_tables,
        dag=dag)


    dag.set_dependency('parse_zte_bulkcm_cm_files', 'clear_zte_bulkcm_cm_tables')
    dag.set_dependency('clear_zte_bulkcm_cm_tables', 'import_zte_bulkcm_parsed_csv')
    dag.set_dependency('import_zte_bulkcm_parsed_csv', 'run_zte_bulkcm_insert_queries')

    return dag