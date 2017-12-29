#
# Insert MO parameters into vendor_parameters table 
#
#@author Bodastage Solutions<info@bodastage.com>
#@licence: Apache 2.0
#
#python /mediation/bin/insert_eri_3g4g_mos_parameters.py /mediation/config/cm/eri_cm_3g4g_parser.conf
#

import psycopg2
import time 
import timeit
import sys 
import os

start_time =  timeit.default_timer()

parser_config_file = schema=sys.argv[1] 

host = os.environ.get('POSTGRES_HOST')

conn = psycopg2.connect("dbname=bts user=bodastage password=password host={0}".format(host))

conn.autocommit = True 

cur = conn.cursor()

with open(parser_config_file, 'r') as f:
	
	for line in f:
		mo_and_params = line.split(":")
		
		print(mo_and_params)
		
		mo = mo_and_params[0].rstrip()
		parameters = mo_and_params[1].split(",")

		lc_parameters = list(map(lambda x: x.rstrip().lower(), parameters))
		
		#Get the index of mo_id
		mo_id = mo + "_id"
		print("------------------------------------------")
		print("mo_: {0}".format(mo_id))
		print(parameters)
		
		index = 0
		try:
			
			index = lc_parameters.index(mo_id.lower())
			print("mo_index: {0}".format(index))
		except:
			continue
			
				
		cur.execute("""select pk FROM managedobjects where "name" = %s """, (mo,))
		row = cur.fetchone()
		mo_pk = row[0]
		
		#Collect parameters to store 
		for idx in range(index,len(parameters)-1):
			parameter = parameters[idx]
			
			#Strip mo_ from parameters 
			parameter = parameter.replace(mo + "_","")

			
			sql = """insert into vendor_parameters
			(pk,"name", notes, date_added, date_modified, added_by, modified_by, tech_pk,vendor_pk,parent_pk)
			values (
			nextval('seq_vendor_parameters_pk'),
			%s,
			%s,
			now()::timestamp,
			now()::timestamp,
			0,
			0,
			2,
			1,
			%s
			)
			"""
			
			cur.execute(sql, (parameter,parameter,mo_pk))
			
elapsed = timeit.default_timer() - start_time
