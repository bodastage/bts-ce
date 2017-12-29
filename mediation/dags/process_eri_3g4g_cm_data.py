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
    dag_id='process_eri_3g4g_cm_data', default_args=args,
    schedule_interval='0 0 * * *',
	max_active_runs = 1,
	concurrency = 1,
	catchup                        = False, 
    dagrun_timeout=timedelta(minutes=60))

	
t1 = BashOperator(
    task_id='check_if_3g4g_raw_files_exist',
    bash_command='ls -1 /mediation/data/cm/ericsson/3g4g/raw/in | wc -l',
    dag=dag)
	
	
t2 = BashOperator(
    task_id='run_eri_3g4g_parser',
    bash_command='java -jar /mediation/bin/boda_bulkcmparser.jar /mediation/data/cm/ericsson/3g4g/raw/in /mediation/data/cm/ericsson/3g4g/parsed/in /mediation/conf/cm/eri_cm_3g4g_parser.cfg',
    dag=dag)
	
#Import csv files into csv files
t3 = BashOperator(
    task_id='import_eri_3g4g_cm_data',
    bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/eri_cm_3g4g_loader.cfg"',
    dag=dag)
	
#Backup raw files that have been parsed
t4 = BashOperator(
    task_id='backup_3g4g_raw_files',
    bash_command='mv -f /mediation/data/cm/ericsson/3g4g/raw/in/* /mediation/data/cm/ericsson/3g4g/raw/out/',
    dag=dag)
	
#Backup previously generate csv files from parsing
t5 = BashOperator(
    task_id='backup_prev_eri_3g4g_csv_files',
    bash_command='mv -f /mediation/data/cm/ericsson/3g4g/parsed/in/* /mediation/data/cm/ericsson/3g4g/parsed/out/',
    dag=dag)
	

#Run network baseline
def generate_network_baseline():
	networkBaseLine = NetworkBaseLine(dbhost=os.environ.get('POSTGRES_HOST'));
	networkBaseLine.run()
	
t6 = PythonOperator(
    task_id='generate_network_baseline',
    python_callable=generate_network_baseline,
    dag=dag)

#Truncate ericsson 3g4g cm tables 
def clear_eri_3g4g_cm_tables():
	utils = Utils(dbhost=os.environ.get('POSTGRES_HOST'));
	utils.truncate_schema_tables(schema="eri_cm_3g4g");
	
t7 = PythonOperator(
	task_id='clear_eri_3g4g_cm_tables',
	python_callable=clear_eri_3g4g_cm_tables,
	dag=dag)
	
#Process ericsson RNCs
def process_eri_rncs():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_rncs()
	
t8 = PythonOperator(
	task_id='process_eri_rncs',
	python_callable=process_eri_rncs,
	dag=dag)
	
#Process ericsson ENodeBs
def process_eri_enodebs():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_enodebs()
	
t9 = PythonOperator(
	task_id='process_eri_enodebs',
	python_callable=process_eri_enodebs,
	dag=dag)
	
#Process Ericsson 3G Sites
def extract_ericsson_3g_sites():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_3g_sites()
	
t10 = PythonOperator(
	task_id='extract_ericsson_3g_sites',
	python_callable=extract_ericsson_3g_sites,
	dag=dag)
	
#Process Ericsson 3G Sites
def extract_ericsson_3g_cells():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_3g_cells()
	
t10 = PythonOperator(
	task_id='extract_ericsson_3g_cells',
	python_callable=extract_ericsson_3g_cells,
	dag=dag)
	
#Process Ericsson 3G Sites
def extract_ericsson_4g_cells():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_4g_cells()
	
t11 = PythonOperator(
	task_id='extract_ericsson_4g_cells',
	python_callable=extract_ericsson_4g_cells,
	dag=dag)

#Process Erisson 3g-2g relations 
def extract_ericsson_3g3g_nbrs():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_3g3g_nbrs()
	
t12 = PythonOperator(
	task_id='extract_ericsson_3g3g_nbrs',
	python_callable=extract_ericsson_3g3g_nbrs,
	dag=dag)
	
#Process Ericsson 4G cell parameters
def extract_ericsson_4g_cell_params():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_4g_cell_params()
	
t13 = PythonOperator(
	task_id='extract_ericsson_4g_cell_params',
	python_callable=extract_ericsson_4g_cell_params,
	dag=dag)
	
#Process Ericsson 3G cell parameters
def extract_ericsson_3g_cell_params():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_3g_cell_params()
	
t14 = PythonOperator(
	task_id='extract_ericsson_3g_cell_params',
	python_callable=extract_ericsson_3g_cell_params,
	dag=dag)
	
#Process Erisson 3g-2g relations 
def extract_ericsson_4g4g_nbrs():
	process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
	process_cm_data.extract_ericsson_4g4g_nbrs()
	
t15 = PythonOperator(
	task_id='extract_ericsson_4g4g_nbrs',
	python_callable=extract_ericsson_4g4g_nbrs,
	dag=dag)

#Build depency graph
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

#Extact ericsson-ericsson 3g-3g nbrs after 3g cells have been extracted 
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g3g_nbrs')

#Extract LTE cell parameter after the cells have been extracted 
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g_cell_params')
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g4g_nbrs')

#Extract UMTS cell parameter after the cells have been extracted 
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g_cell_params')
