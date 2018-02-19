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
from airflow.models import DAG
from datetime import timedelta
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData;

bts_utils = Utils();

schedule_interval = bts_utils.get_setting('cm_dag_schedule_interval')

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
    dag_id='cm_etlp', default_args=args,
    schedule_interval=schedule_interval,
    max_active_runs = 1,
    concurrency = 1,
    catchup = False,
    dagrun_timeout=timedelta(minutes=60))


t1 = BashOperator(
    task_id='check_if_eri_3g4g_raw_files_exist',
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
    bash_command='mv -f /mediation/data/cm/ericsson/3g4g/raw/in/* /mediation/data/cm/ericsson/3g4g/raw/out/ 2>/dev/null',
    dag=dag)

# Backup previously generate csv files from parsing
t5 = BashOperator(
    task_id='backup_prev_eri_3g4g_csv_files',
    bash_command='mv -f /mediation/data/cm/ericsson/3g4g/parsed/in/* /mediation/data/cm/ericsson/3g4g/parsed/out/ 2>/dev/null',
    dag=dag)


# Run network baseline
def generate_eri_3g4g_network_baseline():
    networkBaseLine = NetworkBaseLine(dbhost=os.environ.get('POSTGRES_HOST'));
    networkBaseLine.run(vendor_pk, tech_pk)


t6 = PythonOperator(
    task_id='generate_eri_3g4g_network_baseline',
    python_callable=generate_eri_3g4g_network_baseline,
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
    process_cm_data.extract_ericsson_3g_cells_per_site()


t10 = PythonOperator(
    task_id='extract_ericsson_3g_cells',
    python_callable=extract_ericsson_3g_cells,
    dag=dag)


# Process Ericsson 3G Sites
def extract_ericsson_4g_cells():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_4g_cells_per_site()


t11 = PythonOperator(
    task_id='extract_ericsson_4g_cells',
    python_callable=extract_ericsson_4g_cells,
    dag=dag)


# Process Erisson 3g-2g relations
def extract_ericsson_3g3g_nbrs():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_3g3g_nbrs_per_site()


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
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_2g_cells()


t16 = PythonOperator(
    task_id='extract_ericsson_2g_cells',
    python_callable=extract_ericsson_2g_cells,
    dag=dag)


# Build network tree
def extract_ericsson_2g_sites():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_2g_sites()

t17 = PythonOperator(
    task_id='extract_ericsson_2g_sites',
    python_callable=extract_ericsson_2g_sites,
    dag=dag)


# Process ericsson BSCs
def process_ericsson_bscs():
    process_cm_data = ProcessCMData(dbhost=os.environ.get('POSTGRES_HOST'));
    process_cm_data.extract_ericsson_bscs()


t18 = PythonOperator(
    task_id='process_ericsson_bscs',
    python_callable=process_ericsson_bscs,
    dag=dag)


# Import csv files into csv files
t18 = BashOperator(
    task_id='import_eri_2g_cm_data',
    bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/eri_cm_2g_loader.cfg"',
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
    bash_command='java -jar /mediation/bin/boda-ericssoncnaiparser.jar /mediation/data/cm/ericsson/2g/raw/in /mediation/data/cm/ericsson/2g/parsed/in /mediation/conf/cm/eri_cm_2g_cnaiv2_loader.cfg',
    dag=dag)

# Backup E// 2G raw files that have been parsed
t21 = BashOperator(
    task_id='backup_ericsson_2g_csv_files',
    bash_command='mv -f /mediation/data/cm/ericsson/2g/parsed/in/* /mediation/data/cm/ericsson/2g/parsed/out/ 2>/dev/null',
    dag=dag)

t22 = BashOperator(
    task_id='check_if_2g_raw_files_exist',
    bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/ericsson/2g/raw/in | wc -l` ]; then exit 1; fi',
    dag=dag)


# Task to check whether ericsson is supported in the network
def is_vendor_supported(vendor_id):
    engine = create_engine('postgresql://bodastage:password@database/bts')
    Session = sessionmaker(bind=engine)
    session = Session()
    metadata = MetaData()
    vendors = Table('vendors', metadata, autoload=True, autoload_with=engine)
    vendor = session.query(vendors).filter_by(pk=vendor_id).first()
    session.close()
    if vendor.supported is False:
        return False
    return True


def is_ericsson_supported():
    if is_vendor_supported(1) is True:
        return 'ericsson_is_supported'
    return 'eri_not_supported'


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

t23 = BranchPythonOperator(
    task_id='is_ericsson_supported',
    python_callable=is_ericsson_supported,
    dag=dag)


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


t28 = BashOperator(
    task_id='check_if_hua_2g_raw_files_exist',
    bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/huawei/2g/raw/in | wc -l` ]; then exit 1; fi',
    dag=dag)

t29 = BashOperator(
    task_id='run_huawei_2g_parser',
    bash_command='java -jar /mediation/bin/boda-huaweinbixmlparser.jar /mediation/data/cm/huawei/2g/raw/in /mediation/data/cm/huawei/2g/parsed/in /mediation/conf/cm/hua_cm_2g_nbi_parameters.cfg',
    dag=dag)

t30 = BashOperator(
    task_id='backup_huawei_2g_csv_files',
    bash_command='mv -f /mediation/data/cm/huawei/2g/parsed/in/* /mediation/data/cm/huawei/2g/parsed/out/ 2>/dev/null',
    dag=dag)

# Clear 2G CM data tables
def clear_huawei_2g_cm_tables():
    pass


t31 = PythonOperator(
    task_id='clear_huawei_2g_cm_tables',
    python_callable=clear_huawei_2g_cm_tables,
    dag=dag)

t32 = BashOperator(
    task_id='import_huawei_2g_cm_data',
    bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/hua_cm_2g_nbi_loader.cfg"',
    dag=dag)

# End Extaction Transformation Load Process
t33 = DummyOperator(task_id='end_cm_etlp', dag=dag)

t34 = DummyOperator(task_id='ericsson_is_supported', dag=dag)

t35 = DummyOperator(task_id='huawei_is_supported', dag=dag)

t36 = DummyOperator(task_id='eri_not_supported', dag=dag)

t37 = DummyOperator(task_id='huawei_not_supported', dag=dag)

t38 = DummyOperator(task_id='zte_is_supported', dag=dag)

t39 = DummyOperator(task_id='zte_not_supported', dag=dag)

t40 = DummyOperator(task_id='nokia_is_supported', dag=dag)

t41 = DummyOperator(task_id='nokia_not_supported', dag=dag)

t42 = BashOperator(
    task_id='check_if_hua_3g_raw_files_exist',
    bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/huawei/3g/raw/in | wc -l` ]; then exit 1; fi',
    dag=dag)


t43 = BashOperator(
    task_id='backup_huawei_3g_csv_files',
    bash_command='mv -f /mediation/data/cm/huawei/3g/parsed/in/* /mediation/data/cm/huawei/3g/parsed/out/ 2>/dev/null',
    dag=dag)

t44 = BashOperator(
    task_id='run_huawei_3g_parser',
    bash_command='java -jar /mediation/bin/boda-huaweinbixmlparser.jar /mediation/data/cm/huawei/3g/raw/in /mediation/data/cm/huawei/3g/parsed/in /mediation/conf/cm/hua_cm_3g_nbi_parameters.cfg',
    dag=dag)

# Clear 3G CM data tables
def clear_huawei_3g_cm_tables():
    pass


t45 = PythonOperator(
    task_id='clear_huawei_3g_cm_tables',
    python_callable=clear_huawei_3g_cm_tables,
    dag=dag)


t46 = BashOperator(
    task_id='import_huawei_3g_cm_data',
    bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/hua_cm_3g_nbi_loader.cfg"',
    dag=dag)

t47 = BashOperator(
    task_id='check_if_hua_4g_raw_files_exist',
    bash_command='if [ 0 -eq `ls -1 /mediation/data/cm/huawei/4g/raw/in | wc -l` ]; then exit 1; fi',
    dag=dag)

t48 = BashOperator(
    task_id='backup_huawei_4g_csv_files',
    bash_command='mv -f /mediation/data/cm/huawei/4g/parsed/in/* /mediation/data/cm/huawei/4g/parsed/out/ 2>/dev/null',
    dag=dag)


t49 = BashOperator(
    task_id='run_huawei_4g_parser',
    bash_command='java -jar /mediation/bin/boda-huaweinbixmlparser.jar /mediation/data/cm/huawei/4g/raw/in /mediation/data/cm/huawei/4g/parsed/in /mediation/conf/cm/hua_cm_4g_nbi_parameters.cfg',
    dag=dag)

# Clear 4G CM data tables
def clear_huawei_4g_cm_tables():
    pass


t50 = PythonOperator(
    task_id='clear_huawei_4g_cm_tables',
    python_callable=clear_huawei_4g_cm_tables,
    dag=dag)

t51 = BashOperator(
    task_id='import_huawei_4g_cm_data',
    bash_command='export PGPASSWORD=password && psql -h $POSTGRES_HOST -U bodastage -d bts -a -w -f "/mediation/conf/cm/hua_cm_4g_nbi_loader.cfg"',
    dag=dag)


# extract_ericsson_3g_sites
# Build dependency graph
dag.set_dependency('start_cm_etlp','is_ericsson_supported')
dag.set_dependency('is_ericsson_supported','ericsson_is_supported')
dag.set_dependency('is_ericsson_supported','eri_not_supported')
dag.set_dependency('eri_not_supported','end_cm_etlp')

dag.set_dependency('ericsson_is_supported','check_if_eri_3g4g_raw_files_exist')
dag.set_dependency('check_if_eri_3g4g_raw_files_exist','backup_prev_eri_3g4g_csv_files')
dag.set_dependency('backup_prev_eri_3g4g_csv_files','run_eri_3g4g_parser')
dag.set_dependency('run_eri_3g4g_parser','clear_eri_3g4g_cm_tables')
dag.set_dependency('clear_eri_3g4g_cm_tables','import_eri_3g4g_cm_data')
dag.set_dependency('import_eri_3g4g_cm_data','backup_3g4g_raw_files')
dag.set_dependency('import_eri_3g4g_cm_data','generate_eri_3g4g_network_baseline')
dag.set_dependency('import_eri_3g4g_cm_data','process_eri_rncs')
dag.set_dependency('import_eri_3g4g_cm_data','process_eri_enodebs')
dag.set_dependency('process_eri_rncs','extract_ericsson_3g_sites')
dag.set_dependency('extract_ericsson_3g_sites','extract_ericsson_3g_cells')
dag.set_dependency('process_eri_enodebs','extract_ericsson_4g_cells')

dag.set_dependency('generate_eri_3g4g_network_baseline','end_cm_etlp')
dag.set_dependency('backup_3g4g_raw_files','end_cm_etlp')

# Extact ericsson-ericsson 3g-3g nbrs after 3g cells have been extracted
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g3g_nbrs')
dag.set_dependency('extract_ericsson_3g3g_nbrs','end_cm_etlp')

# Extract LTE cell parameter after the cells have been extracted
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g_cell_params')
dag.set_dependency('extract_ericsson_4g_cells','extract_ericsson_4g4g_nbrs')
dag.set_dependency('extract_ericsson_4g_cell_params','end_cm_etlp')
dag.set_dependency('extract_ericsson_4g4g_nbrs','end_cm_etlp')


# Extract UMTS cell parameter after the cells have been extracted
dag.set_dependency('extract_ericsson_3g_cells','extract_ericsson_3g_cell_params')
dag.set_dependency('extract_ericsson_3g_cell_params','end_cm_etlp')

# ###########################################################################

dag.set_dependency('ericsson_is_supported','check_if_2g_raw_files_exist')
dag.set_dependency('check_if_2g_raw_files_exist','backup_ericsson_2g_csv_files')
dag.set_dependency('backup_ericsson_2g_csv_files','run_ericsson_2g_parser')
dag.set_dependency('run_ericsson_2g_parser','clear_ericsson_2g_cm_tables')
dag.set_dependency('clear_ericsson_2g_cm_tables','import_eri_2g_cm_data')
dag.set_dependency('import_eri_2g_cm_data','process_ericsson_bscs')
dag.set_dependency('process_ericsson_bscs','extract_ericsson_2g_sites')
dag.set_dependency('extract_ericsson_2g_sites','extract_ericsson_2g_cells')

# Build network tree
dag.set_dependency('extract_ericsson_2g_cells','build_network_tree')
dag.set_dependency('extract_ericsson_3g_cells','build_network_tree')
dag.set_dependency('extract_ericsson_4g_cells','build_network_tree')
dag.set_dependency('build_network_tree','end_cm_etlp')

# ###########################################################################
# Huawei
# ##########################################################################
dag.set_dependency('start_cm_etlp','is_huawei_supported')
dag.set_dependency('is_huawei_supported','huawei_is_supported')
dag.set_dependency('is_huawei_supported','huawei_not_supported')
dag.set_dependency('huawei_not_supported','end_cm_etlp')

# Huawei 2G
dag.set_dependency('huawei_is_supported','check_if_hua_2g_raw_files_exist')
dag.set_dependency('check_if_hua_2g_raw_files_exist','backup_huawei_2g_csv_files')
dag.set_dependency('backup_huawei_2g_csv_files','run_huawei_2g_parser')
dag.set_dependency('run_huawei_2g_parser','clear_huawei_2g_cm_tables')
dag.set_dependency('clear_huawei_2g_cm_tables','import_huawei_2g_cm_data')
dag.set_dependency('import_huawei_2g_cm_data','end_cm_etlp')

# Huawei 3G
dag.set_dependency('huawei_is_supported','check_if_hua_3g_raw_files_exist')
dag.set_dependency('check_if_hua_3g_raw_files_exist','backup_huawei_3g_csv_files')
dag.set_dependency('backup_huawei_3g_csv_files','run_huawei_3g_parser')
dag.set_dependency('run_huawei_3g_parser','clear_huawei_3g_cm_tables')
dag.set_dependency('clear_huawei_3g_cm_tables','import_huawei_3g_cm_data')
dag.set_dependency('import_huawei_3g_cm_data','end_cm_etlp')

# Huawei 4G
dag.set_dependency('huawei_is_supported','check_if_hua_4g_raw_files_exist')
dag.set_dependency('check_if_hua_4g_raw_files_exist','backup_huawei_4g_csv_files')
dag.set_dependency('backup_huawei_4g_csv_files','run_huawei_4g_parser')
dag.set_dependency('run_huawei_4g_parser','clear_huawei_4g_cm_tables')
dag.set_dependency('clear_huawei_4g_cm_tables','import_huawei_4g_cm_data')
dag.set_dependency('import_huawei_4g_cm_data','end_cm_etlp')

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