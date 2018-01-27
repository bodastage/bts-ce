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
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import timedelta
from datetime import datetime

sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData;

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
    dag_id='cm_process_data', default_args=args,
    schedule_interval='0 0 * * *',
    max_active_runs = 1,
    concurrency = 1,
    catchup                        = False,
    dagrun_timeout=timedelta(minutes=60))


t1 = BashOperator(
    task_id='check_if_3g4g_raw_files_exist',
    bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/ericsson/3g4g/raw/in | wc -l` ]; then exit 1; fi',
    dag=dag)


t2 = BashOperator(
    task_id='run_eri_3g4g_parser',
    bash_command='java -jar /mediation/bin/boda_bulkcmparser.jar /mediation/data/cm/ericsson/3g4g/raw/in /mediation/data/cm/ericsson/3g4g/parsed/in /mediation/conf/cm/eri_cm_3g4g_parser.cfg',
    dag=dag)

# Import csv files into csv files
t3 = BashOperator(
    task_id='import_eri_3g4g_cm_data',
    bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/eri_cm_3g4g_loader.cfg"',
    dag=dag)

# Backup raw files that have been parsed
t4 = BashOperator(
    task_id='backup_3g4g_raw_files',
    bash_command='mv -f /mediation/data/cm/ericsson/3g4g/raw/in/* /mediation/data/cm/ericsson/3g4g/raw/out/ >/dev/null',
    dag=dag)

# Backup previously generate csv files from parsing
t5 = BashOperator(
    task_id='backup_prev_eri_3g4g_csv_files',
    bash_command='mv -f /mediation/data/cm/ericsson/3g4g/parsed/in/* /mediation/data/cm/ericsson/3g4g/parsed/out/ >/dev/null',
    dag=dag)


# Run network baseline
def generate_network_baseline():
    networkBaseLine = NetworkBaseLine(dbhost=os.environ.get('POSTGRES_HOST'));
    networkBaseLine.run()


t6 = PythonOperator(
    task_id='generate_network_baseline',
    python_callable=generate_network_baseline,
    dag=dag)


# Truncate ericsson 3g4g cm tables
def clear_eri_3g4g_cm_tables():
    utils = Utils(dbhost=os.environ.get('POSTGRES_HOST'));
    utils.truncate_schema_tables(schema="eri_cm_3g4g");


t7 = PythonOperator(
    task_id='clear_eri_3g4g_cm_tables',
    python_callable=clear_eri_3g4g_cm_tables,
    dag=dag)


# Process ericsson RNCs
def process_eri_rncs():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_rncs()


t8 = PythonOperator(
    task_id='process_eri_rncs',
    python_callable=process_eri_rncs,
    dag=dag)


# Process ericsson ENodeBs
def process_eri_enodebs():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_enodebs()


t9 = PythonOperator(
    task_id='process_eri_enodebs',
    python_callable=process_eri_enodebs,
    dag=dag)


# Process Ericsson 3G Sites
def extract_ericsson_3g_sites():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_3g_sites()


t10 = PythonOperator(
    task_id='extract_ericsson_3g_sites',
    python_callable=extract_ericsson_3g_sites,
    dag=dag)


# Process Ericsson 3G Sites
def extract_ericsson_3g_cells():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_3g_cells()


t10 = PythonOperator(
    task_id='extract_ericsson_3g_cells',
    python_callable=extract_ericsson_3g_cells,
    dag=dag)


# Process Ericsson 3G Sites
def extract_ericsson_4g_cells():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_4g_cells()


t11 = PythonOperator(
    task_id='extract_ericsson_4g_cells',
    python_callable=extract_ericsson_4g_cells,
    dag=dag)


# Process Erisson 3g-2g relations
def extract_ericsson_3g3g_nbrs():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_3g3g_nbrs()


t12 = PythonOperator(
    task_id='extract_ericsson_3g3g_nbrs',
    python_callable=extract_ericsson_3g3g_nbrs,
    dag=dag)


# Process Ericsson 4G cell parameters
def extract_ericsson_4g_cell_params():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_4g_cell_params()


t13 = PythonOperator(
    task_id='extract_ericsson_4g_cell_params',
    python_callable=extract_ericsson_4g_cell_params,
    dag=dag)


# Process Ericsson 3G cell parameters
def extract_ericsson_3g_cell_params():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_3g_cell_params()


t14 = PythonOperator(
    task_id='extract_ericsson_3g_cell_params',
    python_callable=extract_ericsson_3g_cell_params,
    dag=dag)


# Process Erisson 3g-2g relations
def extract_ericsson_4g4g_nbrs():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
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
    pass


t16 = PythonOperator(
    task_id='extract_ericsson_2g_cells',
    python_callable=extract_ericsson_2g_cells,
    dag=dag)


# Build network tree
def extract_ericsson_2g_sites():
    pass


t17 = PythonOperator(
    task_id='extract_ericsson_2g_sites',
    python_callable=extract_ericsson_2g_sites,
    dag=dag)


# Process ericsson ENodeBs
def process_ericsson_bscs():
    pass


t18 = PythonOperator(
    task_id='process_ericsson_bscs',
    python_callable=process_ericsson_bscs,
    dag=dag)


# Import E// 2G data
def import_ericsson_2g_cm_data():
    pass


t18 = PythonOperator(
    task_id='import_ericsson_2g_cm_data',
    python_callable=import_ericsson_2g_cm_data,
    dag=dag)

# Clear 2G CM data tables
def clear_ericsson_2g_cm_tables():
    pass


t19 = PythonOperator(
    task_id='clear_ericsson_2g_cm_tables',
    python_callable=clear_ericsson_2g_cm_tables,
    dag=dag)

t20 = BashOperator(
    task_id='run_ericsson_2g_parser',
    bash_command='echo "Parsing E// 2G CM data"',
    dag=dag)

# Backup E// 2G raw files that have been parsed
t21 = BashOperator(
    task_id='backup_ericsson_2g_csv_files',
    bash_command='mv -f /mediation/data/cm/ericsson/2g/parsed/in/* /mediation/data/cm/ericsson/2g/parsed/out/ >/dev/null',
    dag=dag)

t22 = BashOperator(
    task_id='check_if_2g_raw_files_exist',
    bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/ericsson/2g/raw/in | wc -l` ]; then exit 1; fi',
    dag=dag)


# extract_ericsson_3g_sites
# Build dependency graph
dag.set_dependency('check_if_3g4g_raw_files_exist','backup_prev_eri_3g4g_csv_files')	
dag.set_dependency('backup_prev_eri_3g4g_csv_files','run_eri_3g4g_parser')	
dag.set_dependency('run_eri_3g4g_parser','clear_eri_3g4g_cm_tables')
dag.set_dependency('clear_eri_3g4g_cm_tables','import_eri_3g4g_cm_data')
dag.set_dependency('import_eri_3g4g_cm_data','backup_3g4g_raw_files')
dag.set_dependency('import_eri_3g4g_cm_data','generate_network_baseline')
dag.set_dependency('import_eri_3g4g_cm_data','process_eri_rncs')
dag.set_dependency('import_eri_3g4g_cm_data','process_eri_enodebs')
dag.set_dependency('process_eri_rncs','extract_ericsson_3g_sites')
dag.set_dependency('extract_ericsson_3g_sites','extract_ericsson_3g_cells')
dag.set_dependency('process_eri_enodebs','extract_ericsson_4g_cells')

# Extact ericsson-ericsson 3g-3g nbrs after 3g cells have been extracted
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g3g_nbrs')

# Extract LTE cell parameter after the cells have been extracted
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g_cell_params')
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g4g_nbrs')

# Extract UMTS cell parameter after the cells have been extracted
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g_cell_params')


# ###########################################################################

#Check if 2G files exist
dag.set_dependency('check_if_2g_raw_files_exist','backup_ericsson_2g_csv_files')

# Back up any previous csv files
dag.set_dependency('backup_ericsson_2g_csv_files','run_ericsson_2g_parser')

# Parser E// 2G data
dag.set_dependency('run_ericsson_2g_parser','clear_ericsson_2g_cm_tables')

# Clear Ericsson 2G CM data
dag.set_dependency('clear_ericsson_2g_cm_tables','import_ericsson_2g_cm_data')

# Import E// 2G CM data
dag.set_dependency('import_ericsson_2g_cm_data','process_ericsson_bscs')

# Extract E// BSCs
dag.set_dependency('process_ericsson_bscs','extract_ericsson_2g_sites')

# Process E// 2g sites
dag.set_dependency('extract_ericsson_2g_sites','extract_ericsson_2g_cells')

# Build network tree
dag.set_dependency('extract_ericsson_2g_cells','build_network_tree')
dag.set_dependency('extract_ericsson_3g_cells','build_network_tree')
dag.set_dependency('extract_ericsson_4g_cells','build_network_tree')
