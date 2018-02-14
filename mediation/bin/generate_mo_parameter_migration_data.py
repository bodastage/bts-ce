# Extract default mos from from managedobjects
#
#@author Bodastage Solutions<info@bodastage.com>
#@licence: Apache 2.0
#
#python /mediation/bin/generate_vendor_tech_mo_migration_data.py vendor technology

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import time 
import timeit
import sys 
import os

engine = create_engine('postgresql://bodastage:password@database/bts')

schema_name='public'
table_name = 'vendor_parameters'

Session = sessionmaker(bind=engine)
session = Session()
		
metadata = MetaData()

table = Table( table_name, metadata, autoload=True, autoload_with=engine, schema=schema_name)

vendor_parameters = session.query(table).all()

print("""
    op.bulk_insert(vendor_parameters, [
""", end="")
for vp in vendor_parameters:
    print("""
		{{'name': '{0}', 'parent_pk': {1}, 'vendor_pk': {2}, 'tech_pk': {3}, 'modified_by': 0, 'added_by': 0}},
	""".format(vp.name, vp.parent_pk, vp.vendor_pk, vp.tech_pk), end="")
	
print("""
    ])
""", end="")