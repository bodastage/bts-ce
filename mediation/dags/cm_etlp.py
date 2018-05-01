# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import  sys
import os
import airflow
from builtins import range
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import DAG
from datetime import timedelta
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from cm_sub_dag_parse_and_import_eri_3g4g import parse_and_import_eri_3g4g
from cm_sub_dag_parse_and_import_eri_2g import parse_and_import_eri_2g
from cm_sub_dag_parse_and_import_huawei_2g import parse_and_import_huawei_2g
from cm_sub_dag_parse_and_import_huawei_3g import parse_and_import_huawei_3g
from cm_sub_dag_parse_and_import_huawei_4g import parse_and_import_huawei_4g
from cm_sub_dag_parse_and_import_huawei_cfgsyn import parse_and_import_huawei_cfgsyn
from airflow.utils.trigger_rule import TriggerRule

sys.path.append('/mediation/packages')

from bts import NetworkBaseLine, Utils, ProcessCMData

bts_utils = Utils()
process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'))

schedule_interval = "@daily" # # bts_utils.get_setting('cm_dag_schedule_interval')

args = {
    'owner': 'bodastage',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'email': ['support@bodastage.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    dag_id='cm_etlp',
    default_args=args,
    schedule_interval=schedule_interval,
    start_date=datetime(2017, 1, 1),
    max_active_runs = 1,
    concurrency = 1,
    catchup = False,
    dagrun_timeout=timedelta(minutes=60))



sub_dag_parse_and_import_eri_3g4g_cm_files = SubDagOperator(
  subdag=parse_and_import_eri_3g4g('cm_etlp', 'parser_and_import_ericsson_3g4g', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parser_and_import_ericsson_3g4g',
  dag=dag,
)

sub_dag_parse_and_import_eri_2g_cm_files = SubDagOperator(
  subdag=parse_and_import_eri_2g('cm_etlp', 'parser_and_import_ericsson_2g', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parser_and_import_ericsson_2g',
  dag=dag,
)

sub_dag_parse_and_import_huawei_2g_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_2g('cm_etlp', 'parse_and_import_huawei_2g', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_2g',
  dag=dag,
)

sub_dag_parse_and_import_huawei_3g_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_3g('cm_etlp', 'parse_and_import_huawei_3g', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_3g',
  dag=dag,
)

sub_dag_parse_and_import_huawei_4g_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_4g('cm_etlp', 'parse_and_import_huawei_4g', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_4g',
  dag=dag,
)

sub_dag_parse_and_import_huawei_cfgsyn_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_cfgsyn('cm_etlp', 'parse_and_import_huawei_cfgsyn', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_cfgsyn',
  dag=dag,
)

# Backup raw files that have been parsed
t4 = BashOperator(
    task_id='backup_3g4g_raw_files',
    bash_command='mv -f /mediation/data/cm/ericsson/3g4g/raw/in/* /mediation/data/cm/ericsson/3g4g/raw/out/ 2>/dev/null',
    dag=dag)

# Run network baseline
def generate_eri_3g4g_network_baseline():
    networkBaseLine = NetworkBaseLine(dbhost=os.environ.get('POSTGRES_HOST'));
    networkBaseLine.run(vendor_pk, tech_pk)


t6 = PythonOperator(
    task_id='generate_eri_3g4g_network_baseline',
    python_callable=generate_eri_3g4g_network_baseline,
    dag=dag)



# Process ericsson RNCs
def process_eri_rncs():
    process_cm_data.extract_ericsson_rncs()


t8 = PythonOperator(
    task_id='process_eri_rncs',
    python_callable=process_eri_rncs,
    dag=dag)


# Process ericsson ENodeBs
def process_eri_enodebs():
    process_cm_data.extract_ericsson_enodebs()


t9 = PythonOperator(
    task_id='process_eri_enodebs',
    python_callable=process_eri_enodebs,
    dag=dag)


# Process Ericsson 3G Sites
def extract_ericsson_3g_sites():
    process_cm_data.extract_ericsson_3g_sites()


t10 = PythonOperator(
    task_id='extract_ericsson_3g_sites',
    python_callable=extract_ericsson_3g_sites,
    dag=dag)


# Process Ericsson 3G cells
def extract_ericsson_3g_cells():
    process_cm_data.extract_ericsson_3g_cells_per_site()


t10 = PythonOperator(
    task_id='extract_ericsson_3g_cells',
    python_callable=extract_ericsson_3g_cells,
    dag=dag)


# Process Ericsson 4G Sites
def extract_ericsson_4g_cells():
    process_cm_data.extract_ericsson_4g_cells_per_site()


t11 = PythonOperator(
    task_id='extract_ericsson_4g_cells',
    python_callable=extract_ericsson_4g_cells,
    dag=dag)


# Process Erisson 3g-2g relations
def extract_ericsson_3g3g_nbrs():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_3g3g_nbrs_per_site()


t12 = PythonOperator(
    task_id='extract_ericsson_3g3g_nbrs',
    python_callable=extract_ericsson_3g3g_nbrs,
    dag=dag)


# Process Ericsson 4G cell parameters
def extract_ericsson_4g_cell_params():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_4g_cell_params()


t13 = PythonOperator(
    task_id='extract_ericsson_4g_cell_params',
    python_callable=extract_ericsson_4g_cell_params,
    dag=dag)


# Process Ericsson 3G cell parameters
def extract_ericsson_3g_cell_params():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_3g_cell_params()


t14 = PythonOperator(
    task_id='extract_ericsson_3g_cell_params',
    python_callable=extract_ericsson_3g_cell_params,
    dag=dag)


# Process Erisson 3g-2g relations
def extract_ericsson_4g4g_nbrs():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_4g4g_nbrs()


t15 = PythonOperator(
    task_id='extract_ericsson_4g4g_nbrs',
    python_callable=extract_ericsson_4g4g_nbrs,
    dag=dag)


# Build network tree
def build_network_tree():
    utils = Utils(dbhost=os.environ.get('POSTGRES_HOST'));
    utils.build_live_network_aci_tree()


t16 = PythonOperator(
    task_id='build_network_tree',
    python_callable=build_network_tree,
    dag=dag)


# Build network tree
def extract_ericsson_2g_cells():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_2g_cells()


t16 = PythonOperator(
    task_id='extract_ericsson_2g_cells',
    python_callable=extract_ericsson_2g_cells,
    dag=dag)


# Build network tree
def extract_ericsson_2g_sites():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_2g_sites()

t17 = PythonOperator(
    task_id='extract_ericsson_2g_sites',
    python_callable=extract_ericsson_2g_sites,
    dag=dag)


# Process ericsson BSCs
def process_ericsson_bscs():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_bscs()


t18 = PythonOperator(
    task_id='process_ericsson_bscs',
    python_callable=process_ericsson_bscs,
    dag=dag)


def is_ericsson_2g_supported():
    """
    Check if Ericsson 2G is supported
    :return: string parse_and_import_ericsson_2g | ericsson_2g_not_supported
    """
    if bts_utils.is_vendor_and_tech_supported(1,1) is True :
        return 'ericsson_2g_supported'
    else:
        return 'ericsson_2g_not_supported'


def is_ericsson_3g_supported():
    return bts_utils.is_vendor_and_tech_supported(1,2)

def is_ericsson_3g4g_supported():
    if bts_utils.is_vendor_and_tech_supported(1,2) is False and bts_utils.is_vendor_and_tech_supported(1,3) is False:
        return 'ericsson_3g4g_not_supported'
    else:
        return 'ericsson_3g4g_supported'


def is_ericsson_4g_supported():
    return bts_utils.is_vendor_and_tech_supported(1,3)


def is_huawei_2g_supported():
    return bts_utils.is_vendor_and_tech_supported(2,1)


def is_huawei_3g_supported():
    return bts_utils.is_vendor_and_tech_supported(2,2)


def is_huawei_4g_supported():
    return bts_utils.is_vendor_and_tech_supported(2,3)


branch_is_ericsson_3g4g_supported = BranchPythonOperator(
    task_id='is_ericsson_3g4g_supported',
    python_callable=is_ericsson_3g4g_supported,
    dag=dag)

task_ericsson_3g4g_not_supported = DummyOperator(task_id='ericsson_3g4g_not_supported', dag=dag)
task_ericsson_3g4g_supported = DummyOperator(task_id='ericsson_3g4g_supported', dag=dag)

huawei_parsing_done = DummyOperator(task_id='huawei_parsing_done', dag=dag)


# Task to check whether ericsson is supported in the network
def is_vendor_supported(vendor_id):
    engine = create_engine('postgresql://bodastage:password@database/bts')
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata = MetaData()
    vendors_supported = Table('supported_vendor_tech', metadata, autoload=True, autoload_with=engine)
    vendor = session.query(vendors_supported).filter_by(vendor_pk=vendor_id).first()
    session.close()
    if vendor is None:
        return False
    return True


def is_ericsson_supported():
    if is_vendor_supported(1) is True:
        return 'ericsson_is_supported'
    return 'ericsson_not_supported'


def is_huawei_supported():
    if is_vendor_supported(2) is True:
        return 'huawei_is_supported'
    return 'huawei_not_supported'


def is_zte_supported():
    if is_vendor_supported(3) is True:
        return 'zte_is_supported'
    return 'zte_not_supported'


def is_nokia_supported():
    if is_vendor_supported(4) is True:
        return 'nokia_is_supported'
    return 'nokia_not_supported'


# t23 = BranchPythonOperator(
#     task_id='is_ericsson_supported',
#     python_callable=is_ericsson_supported,
#     dag=dag)


# Dummy start task
t24 = DummyOperator(task_id='start_cm_etlp', dag=dag)

t25 = BranchPythonOperator(
    task_id='is_huawei_supported',
    python_callable=is_huawei_supported,
    dag=dag)

t26 = BranchPythonOperator(
    task_id='is_zte_supported',
    python_callable=is_zte_supported,
    dag=dag)


t27 = BranchPythonOperator(
    task_id='is_nokia_supported',
    python_callable=is_nokia_supported,
    dag=dag)

branch_is_ericsson_2g_supported = BranchPythonOperator(
    task_id='is_ericsson_2g_supported',
    python_callable=is_ericsson_2g_supported,
    dag=dag)

task_ericsson_2g_not_supported = DummyOperator(task_id='ericsson_2g_not_supported', dag=dag)
task_ericsson_2g_not_supported = DummyOperator(task_id='ericsson_2g_supported', dag=dag)

# End Extaction Transformation Load Process
t33 = DummyOperator(task_id='end_cm_etlp', dag=dag)

# t34 = DummyOperator(task_id='ericsson_is_supported', dag=dag)

def huawei_is_supported():
    """If Huawei is supported, detect file formats and backup previous csv files"""
    process_cm_data.detect_format_and_move_huawei_cm_raw_files()

    # Backup any left over csv files
    os.system("""
        mv -f /mediation/data/cm/huawei/parsed/{nbi_umts,nbi_sran,nbi_lte,nbi_gsm,mml_umts,mml_lte,mml_gsm,gexport_wcdma,gexport_sran,gexport_other,gexport_lte,gexport_gsm,gexport_cdma,cfgsyn}/* /mediation/data/cm/huawei/parsed/backup 2>/dev/null || true
    """)

t35 = PythonOperator(
    task_id='huawei_is_supported',
    python_callable = huawei_is_supported,
    dag=dag
)

# t36 = DummyOperator(task_id='ericsson_not_supported', dag=dag)

t37 = DummyOperator(task_id='huawei_not_supported', dag=dag)

t38 = DummyOperator(task_id='zte_is_supported', dag=dag)

t39 = DummyOperator(task_id='zte_not_supported', dag=dag)

t40 = DummyOperator(task_id='nokia_is_supported', dag=dag)

t41 = DummyOperator(task_id='nokia_not_supported', dag=dag)


def extract_ericsson_2g_cell_params():
    process_cm_data.extract_ericsson_2g_cell_params()


t52 = PythonOperator(
    task_id='extract_ericsson_2g_cell_params',
    python_callable=extract_ericsson_2g_cell_params,
    dag=dag)

def extract_ericsson_2g2g_nbrs():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_2g2g_nbrs()


t53 = PythonOperator(
    task_id='extract_ericsson_2g2g_nbrs',
    python_callable=extract_ericsson_2g2g_nbrs,
    dag=dag)

# Extract Huawei BSCs
def extract_huawei_bscs():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_huawei_bscs()


t54 = PythonOperator(
    task_id='extract_huawei_bscs',
    python_callable=extract_huawei_bscs,
    dag=dag)

# Build network tree
def extract_huawei_2g_sites():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_huawei_2g_sites()

t56 = PythonOperator(
    task_id='extract_huawei_2g_sites',
    python_callable=extract_huawei_2g_sites,
    dag=dag)

def extract_huawei_2g_cells():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_huawei_2g_cells()


t57 = PythonOperator(
    task_id='extract_huawei_2g_cells',
    python_callable=extract_huawei_2g_cells,
    dag=dag)

def extract_huawei_2g_cell_params():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_huawei_2g_cell_params()


t58 = PythonOperator(
    task_id='extract_huawei_2g_cell_params',
    python_callable=extract_huawei_2g_cell_params,
    dag=dag)

# Process ericsson RNCs
def extract_huawei_rncs():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_huawei_rncs()


t59 = PythonOperator(
    task_id='extract_huawei_rncs',
    python_callable=extract_huawei_rncs,
    dag=dag)

# Process Ericsson 3G Sites
def extract_huawei_3g_sites():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_huawei_3g_sites()

t60 = PythonOperator(
    task_id='extract_huawei_3g_sites',
    python_callable=extract_huawei_3g_sites,
    dag=dag)

# Process Ericsson 3G Sites
def extract_huawei_3g_cells():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_huawei_3g_cells()


t61 = PythonOperator(
    task_id='extract_huawei_3g_cells',
    python_callable=extract_huawei_3g_cells,
    dag=dag)


# Process Huawei 3G cell parameters
def extract_huawei_3g_cell_params():
    process_cm_data.extract_huawei_3g_cell_params()


t62 = PythonOperator(
    task_id='extract_huawei_3g_cell_params',
    python_callable=extract_huawei_3g_cell_params,
    dag=dag)


# Process ericsson ENodeBs
def extract_huawei_enodebs():
    process_cm_data.extract_huawei_enodebs()


t63 = PythonOperator(
    task_id='extract_huawei_enodebs',
    python_callable=extract_huawei_enodebs,
    dag=dag)


# Process Huawei 4G Sites
def extract_huawei_4g_cells():
    process_cm_data.extract_huawei_4g_cells()


t64 = PythonOperator(
    task_id='extract_huawei_4g_cells',
    python_callable=extract_huawei_4g_cells,
    dag=dag)

# Process Huawei 4G Sites
def extract_huawei_4g_cell_params():
    process_cm_data.extract_huawei_4g_cell_params()


t65 = PythonOperator(
    task_id='extract_huawei_4g_cell_params',
    python_callable=extract_huawei_4g_cell_params,
    dag=dag)

def extract_huawei_2g2g_nbrs():
    process_cm_data.extract_huawei_2g2g_nbrs()


t66 = PythonOperator(
    task_id='extract_huawei_2g2g_nbrs',
    python_callable=extract_huawei_2g2g_nbrs,
    dag=dag)

def extract_huawei_2g3g_nbrs():
    process_cm_data.extract_huawei_2g3g_nbrs()


t66 = PythonOperator(
    task_id='extract_huawei_2g3g_nbrs',
    python_callable=extract_huawei_2g3g_nbrs,
    dag=dag)

def extract_huawei_3g3g_nbrs():
    process_cm_data.extract_huawei_3g3g_nbrs()


t67 = PythonOperator(
    task_id='extract_huawei_3g3g_nbrs',
    python_callable=extract_huawei_3g3g_nbrs,
    dag=dag)

def extract_huawei_3g2g_nbrs():
    process_cm_data.extract_huawei_3g2g_nbrs()


t68 = PythonOperator(
    task_id='extract_huawei_3g2g_nbrs',
    python_callable=extract_huawei_3g2g_nbrs,
    dag=dag)


def extract_huawei_3g4g_nbrs():
    process_cm_data.extract_huawei_3g4g_nbrs()


t69 = PythonOperator(
    task_id='extract_huawei_3g4g_nbrs',
    python_callable=extract_huawei_3g4g_nbrs,
    dag=dag)

def extract_huawei_2g4g_nbrs():
    process_cm_data.extract_huawei_2g4g_nbrs()


t69 = PythonOperator(
    task_id='extract_huawei_2g4g_nbrs',
    python_callable=extract_huawei_2g4g_nbrs,
    dag=dag)

def extract_huawei_4g2g_nbrs():
    process_cm_data.extract_huawei_4g2g_nbrs()


t70 = PythonOperator(
    task_id='extract_huawei_4g2g_nbrs',
    python_callable=extract_huawei_4g2g_nbrs,
    dag=dag)

def extract_huawei_4g3g_nbrs():
    process_cm_data.extract_huawei_4g3g_nbrs()


t71 = PythonOperator(
    task_id='extract_huawei_4g3g_nbrs',
    python_callable=extract_huawei_4g3g_nbrs,
    dag=dag)


def extract_huawei_4g4g_nbrs():
    process_cm_data.extract_huawei_4g4g_nbrs()


t72 = PythonOperator(
    task_id='extract_huawei_4g4g_nbrs',
    python_callable=extract_huawei_4g4g_nbrs,
    dag=dag)

def extract_ericsson_2g3g_nbrs():
    process_cm_data.extract_ericsson_2g3g_nbrs()


t73 = PythonOperator(
    task_id='extract_ericsson_2g3g_nbrs',
    python_callable=extract_ericsson_2g3g_nbrs,
    dag=dag)

def extract_ericsson_2g4g_nbrs():
    process_cm_data.extract_ericsson_2g4g_nbrs()


t74 = PythonOperator(
    task_id='extract_ericsson_2g4g_nbrs',
    python_callable=extract_ericsson_2g4g_nbrs,
    dag=dag)

def extract_ericsson_3g2g_nbrs():
    process_cm_data.extract_ericsson_3g2g_nbrs()


t75 = PythonOperator(
    task_id='extract_ericsson_3g2g_nbrs',
    python_callable=extract_ericsson_3g2g_nbrs,
    dag=dag)


def extract_ericsson_3g4g_nbrs():
    process_cm_data.extract_ericsson_3g4g_nbrs()


t76 = PythonOperator(
    task_id='extract_ericsson_3g4g_nbrs',
    python_callable=extract_ericsson_3g4g_nbrs,
    dag=dag)

def extract_ericsson_4g2g_nbrs():
    process_cm_data.extract_ericsson_4g2g_nbrs()

t77 = PythonOperator(
    task_id='extract_ericsson_4g2g_nbrs',
    python_callable=extract_ericsson_4g2g_nbrs,
    dag=dag)

def extract_ericsson_4g3g_nbrs():
    process_cm_data.extract_ericsson_4g3g_nbrs()

t78 = PythonOperator(
    task_id='extract_ericsson_4g3g_nbrs',
    python_callable=extract_ericsson_4g3g_nbrs,
    dag=dag)

t79 = DummyOperator(
    task_id='ericsson_cm_done',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

t80 = DummyOperator(
    task_id='huawei_cm_done',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

t81 = DummyOperator(
    task_id='join_ericsson_supported',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

t82 = DummyOperator(
    task_id='join_huawei_supported',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

# Ericsson


# Build dependency graph
# dag.set_dependency('start_cm_etlp','is_ericsson_supported')
dag.set_dependency('start_cm_etlp','is_ericsson_2g_supported')
dag.set_dependency('start_cm_etlp','is_ericsson_3g4g_supported')

dag.set_dependency('is_ericsson_3g4g_supported','ericsson_3g4g_supported')
dag.set_dependency('is_ericsson_3g4g_supported','ericsson_3g4g_not_supported')

# dag.set_dependency('is_ericsson_supported','ericsson_is_supported')
# dag.set_dependency('is_ericsson_supported','ericsson_not_supported')

# dag.set_dependency('ericsson_not_supported','join_ericsson_supported')
dag.set_dependency('ericsson_cm_done','join_ericsson_supported')
dag.set_dependency('join_ericsson_supported','end_cm_etlp')

dag.set_dependency('ericsson_3g4g_not_supported','ericsson_cm_done')
dag.set_dependency('ericsson_3g4g_supported','parser_and_import_ericsson_3g4g')

# dag.set_dependency('check_if_eri_3g4g_raw_files_exist','backup_prev_eri_3g4g_csv_files')
# dag.set_dependency('backup_prev_eri_3g4g_csv_files','run_eri_3g4g_parser')
# dag.set_dependency('run_eri_3g4g_parser','clear_eri_3g4g_cm_tables')
# dag.set_dependency('clear_eri_3g4g_cm_tables','import_eri_3g4g_cm_data')

dag.set_dependency('parser_and_import_ericsson_3g4g','backup_3g4g_raw_files')
dag.set_dependency('parser_and_import_ericsson_3g4g','generate_eri_3g4g_network_baseline')
dag.set_dependency('parser_and_import_ericsson_3g4g','process_eri_rncs')
dag.set_dependency('parser_and_import_ericsson_3g4g','process_eri_enodebs')
dag.set_dependency('process_eri_rncs','extract_ericsson_3g_sites')
dag.set_dependency('extract_ericsson_3g_sites','extract_ericsson_3g_cells')
dag.set_dependency('process_eri_enodebs','extract_ericsson_4g_cells')

dag.set_dependency('generate_eri_3g4g_network_baseline','ericsson_cm_done')
dag.set_dependency('backup_3g4g_raw_files','ericsson_cm_done')


dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g2g_nbrs')
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g3g_nbrs')
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g4g_nbrs')
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_2g3g_nbrs')
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_4g3g_nbrs')
dag.set_dependency('extract_ericsson_3g2g_nbrs','ericsson_cm_done')
dag.set_dependency('extract_ericsson_3g3g_nbrs','ericsson_cm_done')
dag.set_dependency('extract_ericsson_3g4g_nbrs','ericsson_cm_done')

# Extract LTE cell parameter after the cells have been extracted
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g_cell_params')
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g2g_nbrs')
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g3g_nbrs')
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g4g_nbrs')
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_2g4g_nbrs')
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_3g4g_nbrs')
dag.set_dependency('extract_ericsson_4g_cell_params','ericsson_cm_done')
dag.set_dependency('extract_ericsson_4g2g_nbrs','ericsson_cm_done')
dag.set_dependency('extract_ericsson_4g3g_nbrs','ericsson_cm_done')
dag.set_dependency('extract_ericsson_4g4g_nbrs','ericsson_cm_done')


# Extract UMTS cell parameter after the cells have been extracted
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g_cell_params')
dag.set_dependency('extract_ericsson_3g_cell_params','ericsson_cm_done')

# ###########################################################################
# Ericsson 2G
dag.set_dependency('is_ericsson_2g_supported','ericsson_2g_supported')
dag.set_dependency('ericsson_2g_supported','parser_and_import_ericsson_2g')

dag.set_dependency('is_ericsson_2g_supported','ericsson_2g_not_supported')
dag.set_dependency('ericsson_2g_not_supported','ericsson_cm_done')
# dag.set_dependency('ericsson_is_supported','parser_and_import_ericsson_2g')
dag.set_dependency('parser_and_import_ericsson_2g','process_ericsson_bscs')

dag.set_dependency('process_ericsson_bscs','extract_ericsson_2g_sites')
dag.set_dependency('extract_ericsson_2g_sites','extract_ericsson_2g_cells')
dag.set_dependency('extract_ericsson_2g_cells','extract_ericsson_2g_cell_params')
dag.set_dependency('extract_ericsson_2g_cell_params','ericsson_cm_done')
dag.set_dependency('extract_ericsson_2g_cells','extract_ericsson_2g2g_nbrs')
dag.set_dependency('extract_ericsson_2g_cells','extract_ericsson_2g3g_nbrs')
dag.set_dependency('extract_ericsson_2g_cells','extract_ericsson_2g4g_nbrs')
dag.set_dependency('extract_ericsson_2g_cells','extract_ericsson_3g2g_nbrs')
dag.set_dependency('extract_ericsson_2g_cells','extract_ericsson_4g2g_nbrs')
dag.set_dependency('extract_ericsson_2g2g_nbrs','ericsson_cm_done')
dag.set_dependency('extract_ericsson_2g3g_nbrs','ericsson_cm_done')
dag.set_dependency('extract_ericsson_2g4g_nbrs','ericsson_cm_done')

# Build network tree
dag.set_dependency('extract_ericsson_2g_cells','build_network_tree')
dag.set_dependency('extract_ericsson_3g_cells','build_network_tree')
dag.set_dependency('extract_ericsson_4g_cells','build_network_tree')
dag.set_dependency('build_network_tree','ericsson_cm_done')

# ###########################################################################
# Huawei
# ##########################################################################
dag.set_dependency('start_cm_etlp','is_huawei_supported')
dag.set_dependency('is_huawei_supported','huawei_is_supported')
dag.set_dependency('is_huawei_supported','huawei_not_supported')
dag.set_dependency('huawei_not_supported','join_huawei_supported')
dag.set_dependency('huawei_cm_done','join_huawei_supported')
dag.set_dependency('join_huawei_supported','end_cm_etlp')

# Huawei 2G
dag.set_dependency('huawei_is_supported','parse_and_import_huawei_2g')
dag.set_dependency('parse_and_import_huawei_2g','huawei_parsing_done')
dag.set_dependency('huawei_parsing_done','extract_huawei_bscs')
# dag.set_dependency('parse_and_import_huawei_2g','extract_huawei_bscs')
dag.set_dependency('extract_huawei_bscs','extract_huawei_2g_sites')
dag.set_dependency('extract_huawei_2g_sites','extract_huawei_2g_cells')
dag.set_dependency('extract_huawei_2g_cells','extract_huawei_2g_cell_params')
dag.set_dependency('extract_huawei_2g_cell_params','huawei_cm_done')
dag.set_dependency('extract_huawei_2g_cells','extract_huawei_2g2g_nbrs')
dag.set_dependency('extract_huawei_2g_cells','extract_huawei_2g3g_nbrs')
dag.set_dependency('extract_huawei_2g_cells','extract_huawei_2g4g_nbrs')
dag.set_dependency('extract_huawei_2g_cells','extract_huawei_3g2g_nbrs')
dag.set_dependency('extract_huawei_2g_cells','extract_huawei_4g2g_nbrs')
dag.set_dependency('extract_huawei_2g2g_nbrs','huawei_cm_done')
dag.set_dependency('extract_huawei_2g3g_nbrs','huawei_cm_done')
dag.set_dependency('extract_huawei_2g4g_nbrs','huawei_cm_done')


# Huawei 3G
dag.set_dependency('huawei_is_supported','parse_and_import_huawei_3g')
dag.set_dependency('parse_and_import_huawei_3g','huawei_parsing_done')
dag.set_dependency('huawei_parsing_done','extract_huawei_rncs')
# dag.set_dependency('parse_and_import_huawei_3g','extract_huawei_rncs')
dag.set_dependency('extract_huawei_rncs','extract_huawei_3g_sites')
dag.set_dependency('extract_huawei_3g_sites','extract_huawei_3g_cells')
dag.set_dependency('extract_huawei_3g_cells','extract_huawei_3g_cell_params')
dag.set_dependency('extract_huawei_3g_cell_params','huawei_cm_done')
dag.set_dependency('extract_huawei_3g_cells','extract_huawei_3g2g_nbrs')
dag.set_dependency('extract_huawei_3g_cells','extract_huawei_3g3g_nbrs')
dag.set_dependency('extract_huawei_3g_cells','extract_huawei_3g4g_nbrs')
dag.set_dependency('extract_huawei_3g_cells','extract_huawei_2g3g_nbrs')
dag.set_dependency('extract_huawei_3g_cells','extract_huawei_4g3g_nbrs')
dag.set_dependency('extract_huawei_3g2g_nbrs','huawei_cm_done')
dag.set_dependency('extract_huawei_3g3g_nbrs','huawei_cm_done')
dag.set_dependency('extract_huawei_3g4g_nbrs','huawei_cm_done')

# Huawei 4G
dag.set_dependency('huawei_is_supported','parse_and_import_huawei_4g')
dag.set_dependency('parse_and_import_huawei_4g','huawei_parsing_done')
dag.set_dependency('huawei_parsing_done','extract_huawei_enodebs')
dag.set_dependency('extract_huawei_enodebs','extract_huawei_4g_cells')
dag.set_dependency('extract_huawei_4g_cells','extract_huawei_4g_cell_params')
dag.set_dependency('extract_huawei_4g_cell_params','huawei_cm_done')
dag.set_dependency('extract_huawei_4g_cells','extract_huawei_4g2g_nbrs')
dag.set_dependency('extract_huawei_4g_cells','extract_huawei_4g3g_nbrs')
dag.set_dependency('extract_huawei_4g_cells','extract_huawei_4g4g_nbrs')
dag.set_dependency('extract_huawei_4g_cells','extract_huawei_2g4g_nbrs')
dag.set_dependency('extract_huawei_4g_cells','extract_huawei_3g4g_nbrs')
dag.set_dependency('extract_huawei_4g2g_nbrs','huawei_cm_done')
dag.set_dependency('extract_huawei_4g3g_nbrs','huawei_cm_done')
dag.set_dependency('extract_huawei_4g4g_nbrs','huawei_cm_done')

# Huawei cfgsyn
dag.set_dependency('huawei_is_supported','parse_and_import_huawei_cfgsyn')
dag.set_dependency('parse_and_import_huawei_cfgsyn','huawei_parsing_done')

# ZTE
# ##############################################
dag.set_dependency('start_cm_etlp','is_zte_supported')
dag.set_dependency('is_zte_supported','zte_is_supported')
dag.set_dependency('is_zte_supported','zte_not_supported')
dag.set_dependency('zte_not_supported','end_cm_etlp')
dag.set_dependency('zte_is_supported','end_cm_etlp')

# Nokia
# ##############################################
dag.set_dependency('start_cm_etlp','is_nokia_supported')
dag.set_dependency('is_nokia_supported','nokia_is_supported')
dag.set_dependency('is_nokia_supported','nokia_not_supported')
dag.set_dependency('nokia_not_supported','end_cm_etlp')
dag.set_dependency('nokia_is_supported','end_cm_etlp')