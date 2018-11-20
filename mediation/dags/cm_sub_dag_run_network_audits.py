import  sys
import os
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


sys.path.append('/mediation/packages');

from bts import NetworkBaseLine, Utils, ProcessCMData, HuaweiCM, EricssonCM, NetworkAudit

huawei_cm = HuaweiCM()
ericsson_cm = EricssonCM()


def run_network_audits(parent_dag_name, child_dag_name, start_date, schedule_interval):
    """
    Live network external definitions for all vendors

    :param parent_dag_name:
    :param child_dag_name:
    :param start_date:
    :param schedule_interval:
    :return:
    """
    dag = DAG(
        '%s.%s' % (parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    branch_network_audits_task = DummyOperator( task_id='branch_network_audits', dag=dag)
    join_network_audits_task = DummyOperator(task_id='join_network_audits', dag=dag)

    def base_line_audits():
        """Run network baseline audits"""
        pass

    def inconsistent_gsm_externals():
        """
        Generate GSM external inconsistencies
        """
        network_audit = NetworkAudit()
        network_audit.generate_incosistent_gsm_externals()

    def inconsistent_umts_externals():
        """
        Generate UMTS external inconsistencies
        """
        network_audit = NetworkAudit()
        network_audit.generate_incosistent_umts_externals()

    def inconsistent_lte_externals():
        """
        Generate LTE external inconsistencies
        """
        network_audit = NetworkAudit()
        network_audit.generate_incosistent_lte_externals()

    def generate_missing_one_way_relations():
        network_audit = NetworkAudit()
        network_audit.generate_incosistent_lte_externals()

    def genearate_missing_cosite_relations():
        network_audit = NetworkAudit()
        network_audit.generate_missing_cosite_relations()

    base_line_audits = BranchPythonOperator(
        task_id='base_line_audits',
        python_callable=base_line_audits,
        dag=dag)

    inconsistent_gsm_externals_task = PythonOperator(
        task_id='generate_inconsistent_gsm_externals',
        python_callable=inconsistent_gsm_externals,
        dag=dag)

    inconsistent_umts_externals_task = PythonOperator(
        task_id='generate_inconsistent_umts_externals',
        python_callable=inconsistent_umts_externals,
        dag=dag)

    inconsistent_lte_externals_task = PythonOperator(
        task_id='generate_inconsistent_lte_externals',
        python_callable=inconsistent_lte_externals,
        dag=dag)

    generate_missing_one_way_relations_task = PythonOperator(
        task_id='generate_missing_one_way_relations',
        python_callable=generate_missing_one_way_relations,
        dag=dag)

    genearate_missing_cosite_relations_task = PythonOperator(
        task_id='genearate_missing_cosite_relations',
        python_callable=genearate_missing_cosite_relations,
        dag=dag)

    dag.set_dependency('branch_network_audits', 'base_line_audits')
    dag.set_dependency('branch_network_audits', 'generate_inconsistent_gsm_externals')
    dag.set_dependency('branch_network_audits', 'generate_inconsistent_umts_externals')
    dag.set_dependency('branch_network_audits', 'generate_inconsistent_lte_externals')
    dag.set_dependency('branch_network_audits', 'generate_missing_one_way_relations')
    dag.set_dependency('branch_network_audits', 'genearate_missing_cosite_relations')

    dag.set_dependency('generate_inconsistent_gsm_externals', 'join_network_audits')
    dag.set_dependency('generate_inconsistent_umts_externals', 'join_network_audits')
    dag.set_dependency('generate_inconsistent_lte_externals', 'join_network_audits')
    dag.set_dependency('generate_missing_one_way_relations', 'join_network_audits')
    dag.set_dependency('genearate_missing_cosite_relations', 'join_network_audits')

    dag.set_dependency('base_line_audits', 'join_network_audits')


    return dag