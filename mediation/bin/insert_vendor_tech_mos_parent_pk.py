#
# Extract Ericsson 3G4G MOs from the parser config file and insert them into the managedobjects table 
#
#@author Bodastage Solutions<info@bodastage.com>
#@licence: Apache 2.0
#
#python /mediation/bin/insert_vendor_tech_mos_parent_pk.py vendor technology /mediation/conf/cm/vendor_cm_tech_parser.conf

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

conn = psycopg2.connect("dbname=bts user=bodastage password=password host={0}".format(host))


conn.autocommit = True 

cur = conn.cursor()

		
with open(parser_config_file, 'r') as f:
	
	for line in f:
		mo_and_params = line.split(":")
		mo = mo_and_params[0].rstrip()
		parameters = mo_and_params[1].split(",")

		lc_parameters = list(map(lambda x: x.rstrip().lower(), parameters))
		
		#Get the index of mo_id
		mo_id = mo + "_id"
		print("------------------------------------------")
		print(parameters)
		print("mo:{0} mo_id:{1}".format(mo, mo_id))
		
		try:
			index = lc_parameters.index(mo_id.lower())
			print("mo_index: {0}".format(index))
		except:
			continue
			
		parent_id_index = int(index)-1;
		parent_id = parameters[parent_id_index]
		parent_mo  = parent_id.split("_")[0] #replace("_id", "")
		
		print("parent_id_index:{0}, parent_id:{1}, parent_mo:{2}".format(parent_id_index, parent_id, parent_mo)) 
		
		#Get parent pk 
				
		cur.execute("""select pk FROM managedobjects where UPPER("name") = %s """, (parent_mo.upper(),))
		row = cur.fetchone()
		parent_pk = row[0]
		
		cur.execute("""select parent_pk FROM managedobjects where UPPER("name") = %s """, (mo.upper(),))
		row = cur.fetchone()
		mo_parent_pk = row[0]
		
		print("parent_pk: {0} mo_parent_pk:{1}".format(parent_pk, mo_parent_pk))
		
		if mo_parent_pk != 0: continue
		
		print(""" update managedobjects  set parent_pk = %s  where UPPER("name") = %s  """.format(parent_pk,mo.upper()))
		
		try:
			cur.execute("""
				update managedobjects 
				set parent_pk = %s
				where "name" = %s
			""", (parent_pk,mo,))
		except:
			continue
			
		print("------------------------------------------")
elapsed = timeit.default_timer() - start_time

print( "Execution time: {0}".format(elapsed))