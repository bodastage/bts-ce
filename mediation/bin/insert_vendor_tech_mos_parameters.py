#
# Insert MO parameters into vendor_parameters table 
#
#@author Bodastage Solutions<info@bodastage.com>
#@licence: Apache 2.0
#
#python /mediation/bin/insert_vendor_technology_mos_parameters.py vendor technology /mediation/config/cm/parser.conf
#

import psycopg2
import time 
import timeit
import sys 
import os

start_time =  timeit.default_timer()

vendors = {"ericsson":1,"huawei":2, "zte":3, "nokia":4, "samsung":5, "alcatel":6, "bodastage":7}
technologies = {"gsm":1, "umts":2, "lte":3}

#vendor 
vendor=sys.argv[1] 

#output folder
technology=sys.argv[2] 

vendor_pk = vendors[vendor]
tech_pk = technologies[technology]

parser_config_file = schema=sys.argv[3] 

host = os.environ.get('POSTGRES_HOST')

conn = psycopg2.connect("dbname=bts user=bodastage password=password host=database")

conn.autocommit = True 

cur = conn.cursor()

with open(parser_config_file, 'r') as f:
	
	for line in f:
		mo_and_params = line.split(":")
		
		print(mo_and_params)
		
		mo = mo_and_params[0].rstrip()
		parameters = mo_and_params[1].split(",")

		lc_parameters = list(map(lambda x: x.rstrip().lower(), parameters))
		
		
		if (vendor_pk == 1 and tech_pk == 2) or (vendor_pk == 3) :
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
		else:
		    index = 0
				
		print("""select pk FROM managedobjects where "name" = {0} AND vendor_pk = {1} AND tech_pk = {2}""".format(mo, vendor_pk, tech_pk))
		cur.execute("""select pk FROM managedobjects where "name" = %s AND vendor_pk = %s AND tech_pk = %s""", (mo, vendor_pk, tech_pk))
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
			%s,
			%s,
			%s
			)
			"""

			cur.execute(sql, (parameter,parameter,tech_pk, vendor_pk, mo_pk))
			
elapsed = timeit.default_timer() - start_time
