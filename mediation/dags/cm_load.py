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
from cm_sub_dag_parse_and_import_huawei_gexport import parse_and_import_huawei_gexport
from cm_sub_dag_parse_and_import_huawei_mml import parse_and_import_huawei_mml
from cm_sub_dag_parse_and_import_huawei_nbi import parse_and_import_huawei_nbi
from cm_sub_dag_parse_and_import_huawei_cfgsyn import parse_and_import_huawei_cfgsyn
from cm_sub_dag_parse_and_import_zte_bulkcm import parse_and_import_zte_bulkcm
from cm_sub_dag_parse_and_import_zte_excel import parse_and_import_zte_excel
from cm_sub_dag_parse_and_import_huawei_rnp import parse_and_import_huawei_rnp
from cm_sub_dag_parse_and_import_nokia_raml20 import parse_and_import_nokia_raml20
from airflow.utils.trigger_rule import TriggerRule
from cm_sub_dag_extract_externals import extract_network_externals
from cm_sub_dag_cm_load_house_keeping import run_house_keeping_tasks


sys.path.append('/mediation/packages')

from bts import NetworkBaseLine, Utils, ProcessCMData, HuaweiCM, EricssonCM, ZTECM, NokiaCM

bts_utils = Utils()
process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'))
huawei_cm = HuaweiCM()
ericsson_cm = EricssonCM()
zte_cm = ZTECM()
nokia_cm = NokiaCM()

schedule_interval = "@daily" # # bts_utils.get_setting('cm_dag_schedule_interval')

args = {
    'owner': 'bodastage',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'email': ['support@bodastage.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='cm_load',
    default_args=args,
    schedule_interval=schedule_interval,
    start_date=datetime(2017, 1, 1),
    max_active_runs = 1,
    # concurrency = 1,
    catchup = False,
    dagrun_timeout=timedelta(minutes=24*60)) # dag runs out after 1 day of running


sub_dag_extract_network_externals_task = SubDagOperator(
  subdag=extract_network_externals('cm_load', 'extract_network_externals', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='extract_network_externals',
  dag=dag,
)

sub_dag_cm_load_house_keeping_task = SubDagOperator(
  subdag=run_house_keeping_tasks('cm_load', 'cm_load_house_keeping', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='cm_load_house_keeping',
  dag=dag,
)


sub_dag_parse_and_import_eri_3g4g_cm_files = SubDagOperator(
  subdag=parse_and_import_eri_3g4g('cm_load', 'parse_and_import_ericsson_bulkcm', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_ericsson_bulkcm',
  dag=dag,
)

sub_dag_parse_and_import_eri_2g_cm_files = SubDagOperator(
  subdag=parse_and_import_eri_2g('cm_load', 'parser_and_import_ericsson_2g', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parser_and_import_ericsson_2g',
  dag=dag,
)

sub_dag_parse_and_import_nokia_raml20_cm_files = SubDagOperator(
  subdag=parse_and_import_nokia_raml20('cm_load', 'parse_and_import_nokia_raml20', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_nokia_raml20',
  dag=dag,
)

sub_dag_parse_and_import_huawei_rnp_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_rnp('cm_load', 'parse_and_import_huawei_rnp', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_rnp',
  dag=dag,
)


sub_dag_parse_and_import_huawei_gexport_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_gexport('cm_load', 'parse_and_import_huawei_gexport', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_gexport',
  dag=dag,
)

sub_dag_parse_and_import_huawei_mml_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_mml('cm_load', 'parse_and_import_huawei_mml', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_mml',
  dag=dag,
)

sub_dag_parse_and_import_huawei_nbi_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_nbi('cm_load', 'parse_and_import_huawei_nbi', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_nbi',
  dag=dag,
)

sub_dag_parse_and_import_huawei_cfgsyn_cm_files = SubDagOperator(
  subdag=parse_and_import_huawei_cfgsyn('cm_load', 'parse_and_import_huawei_cfgsyn', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_huawei_cfgsyn',
  dag=dag,
)



sub_dag_parse_and_import_zte_cm_files = SubDagOperator(
  subdag=parse_and_import_zte_bulkcm('cm_load', 'parse_and_import_zte_bulkcm', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_zte_bulkcm',
  dag=dag,
)

sub_dag_parse_and_import_zte_excel_cm_files = SubDagOperator(
  subdag=parse_and_import_zte_excel('cm_load', 'parse_and_import_zte_excel', start_date=dag.start_date,
                 schedule_interval=dag.schedule_interval),
  task_id='parse_and_import_zte_excel',
  dag=dag,
)



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

def extract_ericsson_3g_externals(self):
    """Extract Ericsson 3G externals defined on 2G, 3G and 4G"""
    pass

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


huawei_parsing_done = DummyOperator(task_id='huawei_parsing_done', dag=dag)
zte_parsing_done = DummyOperator(task_id='zte_parsing_done', dag=dag)


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

def process_ericsson():
    """If Ericsson is supported, detect file formats and backup previous csv files"""
    process_cm_data.detect_format_and_move_ericsson_cm_raw_files()

    # Backup any left over csv files
    os.system("""
        mv -f /mediation/data/cm/huawei/parsed/{bulkcm,eaw}/* /mediation/data/cm/huawei/parsed/backup 2>/dev/null || true
    """)

t35 = PythonOperator(
    task_id='process_ericsson',
    python_callable = process_ericsson,
    dag=dag
)


def start_cm_etlp():
    """Do house keeping before dag starts
    1. Insert load into cm_loads table
    2. update is_current_load flag to currently running dag/load
    """
    process_cm_data.register_cm_load()


t_start_cm_etlp = PythonOperator(
    task_id='start_cm_load',
    python_callable=start_cm_etlp,
    dag=dag)



def end_cm_etlp():
    """Do house keeping at the end of the  dag
        - mark the dag/load as completed
    """
    process_cm_data.mark_cm_load_as_completed('SUCCESS')

t_end_cm_etlp = PythonOperator(
    task_id='end_cm_load',
    python_callable=end_cm_etlp,
    dag=dag)


# End Extaction Transformation Load Process
t33 = DummyOperator(task_id='end_cm_load', dag=dag)

def process_huawei_cm():
    """If Huawei is supported, detect file formats and backup previous csv files"""
    process_cm_data.detect_format_and_move_huawei_cm_raw_files()

    # Backup any left over csv files
    os.system("""
        mv -f /mediation/data/cm/huawei/parsed/{nbi,mml,gexport,cfgsyn}/* /mediation/data/cm/huawei/parsed/backup 2>/dev/null || true
    """)

t35 = PythonOperator(
    task_id='process_huawei_cm',
    python_callable = process_huawei_cm,
    dag=dag
)


t38 = DummyOperator(task_id='process_zte', dag=dag)

t40 = DummyOperator(task_id='process_nokia', dag=dag)


cell_extaction_done_task = DummyOperator(task_id='cell_extraction_done', dag=dag)



def extract_ericsson_2g_cell_params():
    process_cm_data.extract_ericsson_2g_cell_params()


t52 = PythonOperator(
    task_id='extract_ericsson_2g_cell_params',
    python_callable=extract_ericsson_2g_cell_params,
    dag=dag)


def extract_ericsson_2g2g_nbrs():
    process_cm_data.extract_ericsson_2g2g_nbrs()


t53 = PythonOperator(
    task_id='extract_ericsson_2g2g_nbrs',
    python_callable=extract_ericsson_2g2g_nbrs,
    dag=dag)


# Extract Huawei BSCs
def extract_huawei_bscs():
    huawei_cm.extract_live_network_bscs()


t54 = PythonOperator(
    task_id='extract_huawei_bscs',
    python_callable=extract_huawei_bscs,
    dag=dag)


# Build network tree
def extract_huawei_2g_sites():
    huawei_cm.extract_live_network_2g_sites()


t56 = PythonOperator(
    task_id='extract_huawei_2g_sites',
    python_callable=extract_huawei_2g_sites,
    dag=dag)


def extract_huawei_2g_cells():
    huawei_cm.extract_live_network_2g_cells()


t57 = PythonOperator(
    task_id='extract_huawei_2g_cells',
    python_callable=extract_huawei_2g_cells,
    dag=dag)

def extract_huawei_2g_cell_params():
    huawei_cm.extract_live_network_2g_cells_params()


t58 = PythonOperator(
    task_id='extract_huawei_2g_cell_params',
    python_callable=extract_huawei_2g_cell_params,
    dag=dag)

# Process ericsson RNCs
def extract_huawei_rncs():
    huawei_cm.extract_live_network_rncs()


t59 = PythonOperator(
    task_id='extract_huawei_rncs',
    python_callable=extract_huawei_rncs,
    dag=dag)

# Process Ericsson 3G Sites
def extract_huawei_3g_sites():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    huawei_cm.extract_live_network_3g_sites()

t60 = PythonOperator(
    task_id='extract_huawei_3g_sites',
    python_callable=extract_huawei_3g_sites,
    dag=dag)

# Process Ericsson 3G Sites
def extract_huawei_3g_cells():
    # process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    huawei_cm.extract_live_network_3g_cells()


t61 = PythonOperator(
    task_id='extract_huawei_3g_cells',
    python_callable=extract_huawei_3g_cells,
    dag=dag)


# Process Huawei 3G cell parameters
def extract_huawei_3g_cell_params():
    huawei_cm.extract_live_network_3g_cell_params()


t62 = PythonOperator(
    task_id='extract_huawei_3g_cell_params',
    python_callable=extract_huawei_3g_cell_params,
    dag=dag)


# Process ericsson ENodeBs
def extract_huawei_enodebs():
    huawei_cm.extract_live_network_enodebs()


t63 = PythonOperator(
    task_id='extract_huawei_enodebs',
    python_callable=extract_huawei_enodebs,
    dag=dag)


# Process Huawei 4G Sites
def extract_huawei_4g_cells():
    huawei_cm.extract_live_network_4g_cells()


t64 = PythonOperator(
    task_id='extract_huawei_4g_cells',
    python_callable=extract_huawei_4g_cells,
    dag=dag)

# Process Huawei 4G Sites
def extract_huawei_4g_cell_params():
    huawei_cm.extract_live_network_4g_cells_params()

t65 = PythonOperator(
    task_id='extract_huawei_4g_cell_params',
    python_callable=extract_huawei_4g_cell_params,
    dag=dag)

def extract_huawei_2g2g_nbrs():
    huawei_cm.extract_live_network_2g2g_nbrs()


t66 = PythonOperator(
    task_id='extract_huawei_2g2g_nbrs',
    python_callable=extract_huawei_2g2g_nbrs,
    dag=dag)



def extract_huawei_2g3g_nbrs():
    huawei_cm.extract_live_network_2g3g_nbrs()


t66 = PythonOperator(
    task_id='extract_huawei_2g3g_nbrs',
    python_callable=extract_huawei_2g3g_nbrs,
    dag=dag)

def extract_huawei_3g3g_nbrs():
    huawei_cm.extract_live_network_3g3g_nbrs()


t67 = PythonOperator(
    task_id='extract_huawei_3g3g_nbrs',
    python_callable=extract_huawei_3g3g_nbrs,
    dag=dag)

def extract_huawei_3g2g_nbrs():
    huawei_cm.extract_live_network_3g2g_nbrs()


t68 = PythonOperator(
    task_id='extract_huawei_3g2g_nbrs',
    python_callable=extract_huawei_3g2g_nbrs,
    dag=dag)


def extract_huawei_3g4g_nbrs():
    huawei_cm.extract_live_network_3g4g_nbrs()


t69 = PythonOperator(
    task_id='extract_huawei_3g4g_nbrs',
    python_callable=extract_huawei_3g4g_nbrs,
    dag=dag)

def extract_huawei_2g4g_nbrs():
    huawei_cm.extract_live_network_2g4g_nbrs()


t69 = PythonOperator(
    task_id='extract_huawei_2g4g_nbrs',
    python_callable=extract_huawei_2g4g_nbrs,
    dag=dag)

def extract_huawei_4g2g_nbrs():
    huawei_cm.extract_live_network_4g2g_nbrs()


t70 = PythonOperator(
    task_id='extract_huawei_4g2g_nbrs',
    python_callable=extract_huawei_4g2g_nbrs,
    dag=dag)


def extract_huawei_4g3g_nbrs():
    huawei_cm.extract_live_network_4g3g_nbrs()


t71 = PythonOperator(
    task_id='extract_huawei_4g3g_nbrs',
    python_callable=extract_huawei_4g3g_nbrs,
    dag=dag)


def extract_huawei_4g4g_nbrs():
    huawei_cm.extract_live_network_4g4g_nbrs()


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

join_ericsson_supported_task = DummyOperator(
    task_id='join_ericsson_supported',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

join_huawei_supported_task = DummyOperator(
    task_id='join_huawei_supported',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)

join_zte_supported_task = DummyOperator(
    task_id='join_zte_supported',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)


def extract_zte_bscs():
    zte_cm.extract_zte_bscs()


task_extract_zte_bscs = PythonOperator(
    task_id='extract_zte_bscs',
    python_callable=extract_zte_bscs,
    dag=dag)


def extract_zte_rncs():
    zte_cm.extract_zte_rncs()


task_extract_zte_rncs = PythonOperator(
    task_id='extract_zte_rncs',
    python_callable=extract_zte_rncs,
    dag=dag)


def extract_zte_enodes():
    zte_cm.extract_zte_enodes()


task_extract_zte_enodes = PythonOperator(
    task_id='extract_zte_enodes',
    python_callable=extract_zte_enodes,
    dag=dag)


def extract_zte_2g_sites():
    zte_cm.extract_zte_2g_sites()


task_extract_zte_2g_sites = PythonOperator(
    task_id='extract_zte_2g_sites',
    python_callable=extract_zte_2g_sites,
    dag=dag)

def extract_zte_2g_cells():
    zte_cm.extract_zte_2g_cells()


task_extract_zte_2g_cells = PythonOperator(
    task_id='extract_zte_2g_cells',
    python_callable=extract_zte_2g_cells,
    dag=dag)


def extract_zte_2g_cell_params():
    zte_cm.extract_zte_2g_cell_params()


task_extract_zte_2g_cell_params = PythonOperator(
    task_id='extract_zte_2g_cell_params',
    python_callable=extract_zte_2g_cell_params,
    dag=dag)


def extract_zte_3g_sites():
    zte_cm.extract_zte_3g_sites()


task_extract_zte_3g_sites = PythonOperator(
    task_id='extract_zte_3g_sites',
    python_callable=extract_zte_3g_sites,
    dag=dag)

def extract_zte_3g_cells():
    zte_cm.extract_zte_3g_cells()


task_extract_zte_3g_cells = PythonOperator(
    task_id='extract_zte_3g_cells',
    python_callable=extract_zte_3g_cells,
    dag=dag)


def extract_zte_3g_cell_params():
    zte_cm.extract_zte_3g_cell_params()


task_extract_zte_3g_cell_params = PythonOperator(
    task_id='extract_zte_3g_cell_params',
    python_callable=extract_zte_3g_cell_params,
    dag=dag)


def extract_zte_4g_cells():
    zte_cm.extract_zte_4g_cells()


task_extract_zte_4g_cells = PythonOperator(
    task_id='extract_zte_4g_cells',
    python_callable=extract_zte_4g_cells,
    dag=dag)


def extract_zte_4g_cell_params():
    zte_cm.extract_zte_4g_cell_params()


task_extract_zte_4g_cell_params = PythonOperator(
    task_id='extract_zte_4g_cell_params',
    python_callable=extract_zte_4g_cell_params,
    dag=dag)


def extract_zte_2g2g_nbrs():
    zte_cm.extract_zte_2g2g_nbrs()


task_extract_zte_2g2g_nbrs = PythonOperator(
    task_id='extract_zte_2g2g_nbrs',
    python_callable=extract_zte_2g2g_nbrs,
    dag=dag)


def extract_zte_2g3g_nbrs():
    zte_cm.extract_zte_2g3g_nbrs()


task_extract_zte_2g3g_nbrs = PythonOperator(
    task_id='extract_zte_2g3g_nbrs',
    python_callable=extract_zte_2g3g_nbrs,
    dag=dag)


def extract_zte_2g4g_nbrs():
    zte_cm.extract_zte_2g4g_nbrs()


task_extract_zte_2g4g_nbrs = PythonOperator(
    task_id='extract_zte_2g4g_nbrs',
    python_callable=extract_zte_2g4g_nbrs,
    dag=dag)


def extract_zte_3g2g_nbrs():
    zte_cm.extract_zte_3g2g_nbrs()


task_extract_zte_3g2g_nbrs = PythonOperator(
    task_id='extract_zte_3g2g_nbrs',
    python_callable=extract_zte_3g2g_nbrs,
    dag=dag)


def extract_zte_3g3g_nbrs():
    zte_cm.extract_zte_3g3g_nbrs()


task_extract_zte_3g3g_nbrs = PythonOperator(
    task_id='extract_zte_3g3g_nbrs',
    python_callable=extract_zte_3g3g_nbrs,
    dag=dag)


def extract_zte_3g4g_nbrs():
    zte_cm.extract_zte_3g4g_nbrs()


task_extract_zte_3g4g_nbrs = PythonOperator(
    task_id='extract_zte_3g4g_nbrs',
    python_callable=extract_zte_3g4g_nbrs,
    dag=dag)


def extract_zte_4g2g_nbrs():
    zte_cm.extract_zte_4g2g_nbrs()


task_extract_zte_4g2g_nbrs = PythonOperator(
    task_id='extract_zte_4g2g_nbrs',
    python_callable=extract_zte_4g2g_nbrs,
    dag=dag)


def extract_zte_4g3g_nbrs():
    zte_cm.extract_zte_4g3g_nbrs()


task_extract_zte_4g3g_nbrs = PythonOperator(
    task_id='extract_zte_4g3g_nbrs',
    python_callable=extract_zte_4g3g_nbrs,
    dag=dag)


def extract_zte_4g4g_nbrs():
    zte_cm.extract_zte_4g4g_nbrs()


task_extract_zte_4g4g_nbrs = PythonOperator(
    task_id='extract_zte_4g4g_nbrs',
    python_callable=extract_zte_4g4g_nbrs,
    dag=dag)

task_zte_cm_done = DummyOperator(task_id='zte_cm_done', dag=dag)


def extract_nokia_bscs():
    nokia_cm.extract_live_network_bscs()


task_extract_nokia_bscs = PythonOperator(
    task_id='extract_nokia_bscs',
    python_callable=extract_nokia_bscs,
    dag=dag)


def extract_nokia_rncs():
    nokia_cm.extract_live_network_rncs()


task_extract_nokia_rncs = PythonOperator(
    task_id='extract_nokia_rncs',
    python_callable=extract_nokia_rncs,
    dag=dag)


def extract_nokia_enodebs():
    nokia_cm.extract_live_network_enodebs()


task_extract_nokia_enodes = PythonOperator(
    task_id='extract_nokia_enodebs',
    python_callable=extract_nokia_enodebs,
    dag=dag)


def extract_nokia_2g_sites():
    nokia_cm.extract_live_network_2g_sites()


task_extract_nokia_2g_sites = PythonOperator(
    task_id='extract_nokia_2g_sites',
    python_callable=extract_nokia_2g_sites,
    dag=dag)

def extract_nokia_2g_cells():
    nokia_cm.extract_live_network_2g_cells()


task_extract_nokia_2g_cells = PythonOperator(
    task_id='extract_nokia_2g_cells',
    python_callable=extract_nokia_2g_cells,
    dag=dag)


def extract_nokia_2g_cell_params():
    nokia_cm.extract_live_network_2g_cell_params()


task_extract_nokia_2g_cell_params = PythonOperator(
    task_id='extract_nokia_2g_cell_params',
    python_callable=extract_nokia_2g_cell_params,
    dag=dag)


def extract_nokia_3g_sites():
    nokia_cm.extract_live_network_3g_sites()


task_extract_nokia_3g_sites = PythonOperator(
    task_id='extract_nokia_3g_sites',
    python_callable=extract_nokia_3g_sites,
    dag=dag)

def extract_nokia_3g_cells():
    nokia_cm.extract_live_network_3g_cells()


task_extract_nokia_3g_cells = PythonOperator(
    task_id='extract_nokia_3g_cells',
    python_callable=extract_nokia_3g_cells,
    dag=dag)


def extract_nokia_3g_cell_params():
    nokia_cm.extract_live_network_3g_cell_params()


task_extract_nokia_3g_cell_params = PythonOperator(
    task_id='extract_nokia_3g_cell_params',
    python_callable=extract_nokia_3g_cell_params,
    dag=dag)


def extract_nokia_4g_cells():
    nokia_cm.extract_live_network_4g_cells()


task_extract_nokia_4g_cells = PythonOperator(
    task_id='extract_nokia_4g_cells',
    python_callable=extract_nokia_4g_cells,
    dag=dag)


def extract_nokia_4g_cell_params():
    nokia_cm.extract_live_network_4g_cell_params()


task_extract_nokia_4g_cell_params = PythonOperator(
    task_id='extract_nokia_4g_cell_params',
    python_callable=extract_nokia_4g_cell_params,
    dag=dag)


def extract_nokia_2g2g_nbrs():
    nokia_cm.extract_live_network_2g2g_nbrs()


task_extract_nokia_2g2g_nbrs = PythonOperator(
    task_id='extract_nokia_2g2g_nbrs',
    python_callable=extract_nokia_2g2g_nbrs,
    dag=dag)


def extract_nokia_2g3g_nbrs():
    nokia_cm.extract_live_network_2g3g_nbrs()


task_extract_nokia_2g3g_nbrs = PythonOperator(
    task_id='extract_nokia_2g3g_nbrs',
    python_callable=extract_nokia_2g3g_nbrs,
    dag=dag)


def extract_nokia_2g4g_nbrs():
    nokia_cm.extract_live_network_2g4g_nbrs()


task_extract_nokia_2g4g_nbrs = PythonOperator(
    task_id='extract_nokia_2g4g_nbrs',
    python_callable=extract_nokia_2g4g_nbrs,
    dag=dag)


def extract_nokia_3g2g_nbrs():
    nokia_cm.extract_live_network_3g2g_nbrs()


task_extract_nokia_3g2g_nbrs = PythonOperator(
    task_id='extract_nokia_3g2g_nbrs',
    python_callable=extract_nokia_3g2g_nbrs,
    dag=dag)


def extract_nokia_3g3g_nbrs():
    nokia_cm.extract_live_network_3g3g_nbrs()


task_extract_nokia_3g3g_nbrs = PythonOperator(
    task_id='extract_nokia_3g3g_nbrs',
    python_callable=extract_nokia_3g3g_nbrs,
    dag=dag)


def extract_nokia_3g4g_nbrs():
    nokia_cm.extract_live_network_3g4g_nbrs()


task_extract_nokia_3g4g_nbrs = PythonOperator(
    task_id='extract_nokia_3g4g_nbrs',
    python_callable=extract_nokia_3g4g_nbrs,
    dag=dag)


def extract_nokia_4g2g_nbrs():
    nokia_cm.extract_live_network_4g2g_nbrs()


task_extract_nokia_4g2g_nbrs = PythonOperator(
    task_id='extract_nokia_4g2g_nbrs',
    python_callable=extract_nokia_4g2g_nbrs,
    dag=dag)


def extract_nokia_4g3g_nbrs():
    nokia_cm.extract_live_network_4g3g_nbrs()


task_extract_nokia_4g3g_nbrs = PythonOperator(
    task_id='extract_nokia_4g3g_nbrs',
    python_callable=extract_nokia_4g3g_nbrs,
    dag=dag)


def extract_nokia_4g4g_nbrs():
    nokia_cm.extract_live_network_4g4g_nbrs()


task_extract_nokia_4g4g_nbrs = PythonOperator(
    task_id='extract_nokia_4g4g_nbrs',
    python_callable=extract_nokia_4g4g_nbrs,
    dag=dag)

task_nokia_cm_done = DummyOperator(task_id='nokia_cm_done', dag=dag)

join_nokia_supported_task = DummyOperator(
    task_id='join_nokia_supported',
    trigger_rule=TriggerRule.ONE_SUCCESS,
    dag=dag
)


# Build dependency graph
# dag.set_dependency('start_cm_load','is_ericsson_supported')
dag.set_dependency('start_cm_load','process_ericsson')

dag.set_dependency('ericsson_cm_done','join_ericsson_supported')
dag.set_dependency('join_ericsson_supported','end_cm_load')

dag.set_dependency('process_ericsson','parse_and_import_ericsson_bulkcm')

dag.set_dependency('parse_and_import_ericsson_bulkcm','process_eri_rncs')
dag.set_dependency('parse_and_import_ericsson_bulkcm','process_eri_enodebs')
dag.set_dependency('process_eri_rncs','extract_ericsson_3g_sites')
dag.set_dependency('extract_ericsson_3g_sites','extract_ericsson_3g_cells')
dag.set_dependency('process_eri_enodebs','extract_ericsson_4g_cells')
dag.set_dependency('extract_ericsson_3g_cells','cell_extraction_done')



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

dag.set_dependency('extract_ericsson_4g_cells','cell_extraction_done')


# Extract UMTS cell parameter after the cells have been extracted
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g_cell_params')
dag.set_dependency('extract_ericsson_3g_cell_params','ericsson_cm_done')

# ###########################################################################
# Ericsson 2G
dag.set_dependency('process_ericsson','parser_and_import_ericsson_2g')

# dag.set_dependency('process_ericsson','parser_and_import_ericsson_2g')
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



dag.set_dependency('extract_ericsson_2g_cells','cell_extraction_done')

# ###########################################################################
# Huawei
# ##########################################################################
dag.set_dependency('start_cm_load','process_huawei_cm')
dag.set_dependency('huawei_cm_done','join_huawei_supported')
dag.set_dependency('join_huawei_supported','end_cm_load')

dag.set_dependency('process_huawei_cm','parse_and_import_huawei_mml')
dag.set_dependency('parse_and_import_huawei_mml','huawei_parsing_done')

dag.set_dependency('process_huawei_cm','parse_and_import_huawei_nbi')
dag.set_dependency('parse_and_import_huawei_nbi','huawei_parsing_done')

# Huawei 2G
dag.set_dependency('huawei_parsing_done','extract_huawei_bscs')

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

dag.set_dependency('extract_huawei_2g_cells','cell_extraction_done')


# Huawei 3G

dag.set_dependency('huawei_parsing_done','extract_huawei_rncs')
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

dag.set_dependency('extract_huawei_3g_cells','cell_extraction_done')

# Huawei 4G
dag.set_dependency('process_huawei_cm','parse_and_import_huawei_gexport')
dag.set_dependency('parse_and_import_huawei_gexport','huawei_parsing_done')
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

dag.set_dependency('extract_huawei_4g_cells','cell_extraction_done')

# Huawei Radio Network Planning Template Data
dag.set_dependency('process_huawei_cm','parse_and_import_huawei_rnp')
dag.set_dependency('parse_and_import_huawei_rnp','huawei_parsing_done')

# Huawei cfgsyn
dag.set_dependency('process_huawei_cm','parse_and_import_huawei_cfgsyn')
dag.set_dependency('parse_and_import_huawei_cfgsyn','huawei_parsing_done')

# ZTE
# ##############################################
dag.set_dependency('start_cm_load','process_zte')
dag.set_dependency('process_zte','parse_and_import_zte_bulkcm')
dag.set_dependency('process_zte','parse_and_import_zte_excel')
dag.set_dependency('parse_and_import_zte_bulkcm','zte_parsing_done')
dag.set_dependency('parse_and_import_zte_excel','zte_parsing_done')

dag.set_dependency('zte_parsing_done','extract_zte_bscs')
dag.set_dependency('zte_parsing_done','extract_zte_rncs')
dag.set_dependency('zte_parsing_done','extract_zte_enodes')

dag.set_dependency('extract_zte_bscs','extract_zte_2g_sites')
dag.set_dependency('extract_zte_2g_sites','extract_zte_2g_cells')
dag.set_dependency('extract_zte_2g_cells','extract_zte_2g_cell_params')
dag.set_dependency('extract_zte_2g_cell_params','zte_cm_done')

dag.set_dependency('extract_zte_rncs','extract_zte_3g_sites')
dag.set_dependency('extract_zte_3g_sites','extract_zte_3g_cells')
dag.set_dependency('extract_zte_3g_cells','extract_zte_3g_cell_params')
dag.set_dependency('extract_zte_3g_cell_params','zte_cm_done')

dag.set_dependency('extract_zte_enodes','extract_zte_4g_cells')
dag.set_dependency('extract_zte_4g_cells','extract_zte_4g_cell_params')
dag.set_dependency('extract_zte_4g_cell_params','zte_cm_done')

dag.set_dependency('extract_zte_2g_cells','extract_zte_2g2g_nbrs')
dag.set_dependency('extract_zte_2g_cells','extract_zte_2g3g_nbrs')
dag.set_dependency('extract_zte_2g_cells','extract_zte_2g4g_nbrs')

dag.set_dependency('extract_zte_3g_cells','extract_zte_3g2g_nbrs')
dag.set_dependency('extract_zte_3g_cells','extract_zte_3g3g_nbrs')
dag.set_dependency('extract_zte_3g_cells','extract_zte_3g4g_nbrs')

dag.set_dependency('extract_zte_4g_cells','extract_zte_4g2g_nbrs')
dag.set_dependency('extract_zte_4g_cells','extract_zte_4g3g_nbrs')
dag.set_dependency('extract_zte_4g_cells','extract_zte_4g4g_nbrs')

dag.set_dependency('extract_zte_2g2g_nbrs','zte_cm_done')
dag.set_dependency('extract_zte_2g3g_nbrs','zte_cm_done')
dag.set_dependency('extract_zte_2g4g_nbrs','zte_cm_done')

dag.set_dependency('extract_zte_3g2g_nbrs','zte_cm_done')
dag.set_dependency('extract_zte_3g3g_nbrs','zte_cm_done')
dag.set_dependency('extract_zte_3g4g_nbrs','zte_cm_done')


dag.set_dependency('extract_zte_4g2g_nbrs','zte_cm_done')
dag.set_dependency('extract_zte_4g3g_nbrs','zte_cm_done')
dag.set_dependency('extract_zte_4g4g_nbrs','zte_cm_done')

dag.set_dependency('zte_cm_done','join_zte_supported')
dag.set_dependency('join_zte_supported','end_cm_load')

# Nokia
# ##############################################
dag.set_dependency('start_cm_load','process_nokia')

dag.set_dependency('process_nokia','parse_and_import_nokia_raml20')

dag.set_dependency('parse_and_import_nokia_raml20','extract_nokia_bscs')
dag.set_dependency('parse_and_import_nokia_raml20','extract_nokia_rncs')
dag.set_dependency('parse_and_import_nokia_raml20','extract_nokia_enodebs')

dag.set_dependency('extract_nokia_bscs','extract_nokia_2g_sites')
dag.set_dependency('extract_nokia_2g_sites','extract_nokia_2g_cells')
dag.set_dependency('extract_nokia_2g_cells','extract_nokia_2g_cell_params')
dag.set_dependency('extract_nokia_2g_cell_params','nokia_cm_done')

dag.set_dependency('extract_nokia_rncs','extract_nokia_3g_sites')
dag.set_dependency('extract_nokia_3g_sites','extract_nokia_3g_cells')
dag.set_dependency('extract_nokia_3g_cells','extract_nokia_3g_cell_params')
dag.set_dependency('extract_nokia_3g_cell_params','nokia_cm_done')

dag.set_dependency('extract_nokia_enodebs','extract_nokia_4g_cells')
dag.set_dependency('extract_nokia_4g_cells','extract_nokia_4g_cell_params')
dag.set_dependency('extract_nokia_4g_cell_params','nokia_cm_done')

dag.set_dependency('extract_nokia_2g_cells','extract_nokia_2g2g_nbrs')
dag.set_dependency('extract_nokia_2g_cells','extract_nokia_2g3g_nbrs')
dag.set_dependency('extract_nokia_2g_cells','extract_nokia_2g4g_nbrs')

dag.set_dependency('extract_nokia_3g_cells','extract_nokia_3g2g_nbrs')
dag.set_dependency('extract_nokia_3g_cells','extract_nokia_3g3g_nbrs')
dag.set_dependency('extract_nokia_3g_cells','extract_nokia_3g4g_nbrs')

dag.set_dependency('extract_nokia_4g_cells','extract_nokia_4g2g_nbrs')
dag.set_dependency('extract_nokia_4g_cells','extract_nokia_4g3g_nbrs')
dag.set_dependency('extract_nokia_4g_cells','extract_nokia_4g4g_nbrs')

dag.set_dependency('extract_nokia_2g2g_nbrs','nokia_cm_done')
dag.set_dependency('extract_nokia_2g3g_nbrs','nokia_cm_done')
dag.set_dependency('extract_nokia_2g4g_nbrs','nokia_cm_done')

dag.set_dependency('extract_nokia_3g2g_nbrs','nokia_cm_done')
dag.set_dependency('extract_nokia_3g3g_nbrs','nokia_cm_done')
dag.set_dependency('extract_nokia_3g4g_nbrs','nokia_cm_done')


dag.set_dependency('extract_nokia_4g2g_nbrs','nokia_cm_done')
dag.set_dependency('extract_nokia_4g3g_nbrs','nokia_cm_done')
dag.set_dependency('extract_nokia_4g4g_nbrs','nokia_cm_done')

dag.set_dependency('nokia_cm_done','join_nokia_supported')
dag.set_dependency('join_nokia_supported','end_cm_load')

# After
dag.set_dependency('cell_extraction_done','end_cm_load')


dag.set_dependency('cell_extraction_done', 'extract_network_externals')
dag.set_dependency('extract_network_externals', 'end_cm_load')

dag.set_dependency('end_cm_load', 'cm_load_house_keeping')
