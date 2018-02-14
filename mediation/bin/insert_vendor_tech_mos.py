#
# Extract Ericsson 3G4G MOs from the parser config file and insert them into the managedobjects table 
#
#@author Bodastage Solutions<info@bodastage.com>
#@licence: Apache 2.0
#
#bin\python bin\utils\insert_vendor_tech_mos.py vendor technology config\cm\eri_cm_3g4g_parser.conf
#

import psycopg2
import time 
import timeit
import sys 


print( sys.argv )
print ("---------------------------")

vendors = {"ericsson":1,"huawei":2, "zte":3, "nokia":4, "samsung":5, "alcatel":6, "bodastage":7}
technologies = {"gsm":1, "umts":2, "lte":3}

#vendor 
vendor=sys.argv[1] 

#output folder
technology=sys.argv[2] 

vendor_pk = vendors[vendor]
tech_pk = technologies[technology]

start_time =  timeit.default_timer()

parser_config_file = schema=sys.argv[3] 

conn = psycopg2.connect("host=database dbname=bts user=bodastage password=password")

conn.autocommit = True 

cur = conn.cursor()

		
with open(parser_config_file, 'r') as f:
	
	for line in f:
		mo_and_params = line.split(":")
		mo = mo_and_params[0]
		print(mo)
		cur.execute("""select count(1) as count FROM managedobjects where "name" = %s """, (mo,))
		row = cur.fetchone()
		count = row[0]

		if count == 0:
			cur.execute("""
				INSERT INTO managedobjects
				(pk, added_by,date_added,date_modified,modified_by, "name", notes, parent_pk, tech_pk, vendor_pk)
				VALUES (
				nextval('seq_managedobjects_pk'),
				0,
				now()::timestamp,
				now()::timestamp,
				0,
				%s,
				%s,
				0, -- parent pk
				%s,
				%s
				);
			""", (mo,mo,tech_pk, vendor_pk,))

sql="""

"""

#cur.execute( sql)

		
elapsed = timeit.default_timer() - start_time

print( "Execution time: {0}".format(elapsed))