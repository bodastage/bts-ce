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
table_name = 'managedobjects'

vendors = {"ericsson":1,"huawei":2, "zte":3, "nokia":4, "samsung":5, "alcatel":6, "bodastage":7}
technologies = {"gsm":1, "umts":2, "lte":3}

#vendor 
vendor=sys.argv[1] 

#output folder
technology=sys.argv[2] 

vendor_pk = vendors[vendor]
tech_pk = technologies[technology]

Session = sessionmaker(bind=engine)
session = Session()
		
metadata = MetaData()

table = Table( table_name, metadata, autoload=True, autoload_with=engine, schema=schema_name)

managedobjects = session.query(table).filter_by(vendor_pk=vendor_pk).filter_by(tech_pk=tech_pk).all()

print("""
    op.bulk_insert(managedobjects, [
""", end="")
for mo in managedobjects:
    print("""
		{{'name': '{0}', 'parent_pk': {1}, 'vendor_pk': {2}, 'tech_pk': {3}, 'modified_by': 0, 'added_by': 0}},
	""".format(mo.name, mo.parent_pk, mo.vendor_pk, mo.tech_pk), end="")
	
print("""
    ])
""", end="")