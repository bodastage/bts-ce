# Run insert queries for cm data
# run_cm_load_insert_queries.py file_format
# 
# file_format = huawei_gexport_gsm, etc...
import os
import sys
import csv
import subprocess
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

if len(sys.argv) != 2: 
	print("Format: {0} {1} {2}".format( os.path.basename(__file__)))
	sys.exit()

file_format=sys.argv[1]

SQLALCHEMY_DATABASE_URI = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(
							os.getenv("BTS_DB_USER", "bodastage"),
							os.getenv("BTS_DB_PASS", "password"),
							os.getenv("BTS_DB_HOST", "192.168.99.100"),
							os.getenv("BTS_DB_PORT", "5432"),
							os.getenv("BTS_DB_NAME", "bts"),
						)
						
engine = create_engine(SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData()

# Loader environment variables
loader_env = {}
loader_env["PGPASSWORD"] = 'password'

sql = "SELECT insert_query FROM cm_load_insert_queries WHERE file_format = '{}' ".format(file_format)

result = engine.execute(sql)

for lq in  result.fetchall():
    query = "{} {}".format( lq[0],";")
    print("Inserting {} data...".format(file_format))
    print("{}".format(query))
    engine.execute(query)

    # subprocess.call(["psql", "-U", "bodastage", "-d", "bts","-h","database" ,"-c", query], env=loader_env)
