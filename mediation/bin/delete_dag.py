import sqlite3
import sys
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

engine = create_engine('postgresql://airflow:airflow@database/airflow')

Session = sessionmaker(engine)
session = Session()

dag_id = sys.argv[1]

for t in ["xcom", "task_instance", "sla_miss", "log", "job", "dag_run", "dag" ]:
    query = "delete from {} where dag_id='{}'".format(t, dag_id)
    session.execute(query)

session.commit()
session.close()