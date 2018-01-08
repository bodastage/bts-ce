#
# Extract Ericsson 3G4G MOs from the parser config file and insert them into the managedobjects table 
#
#@author Bodastage Solutions<info@bodastage.com>
#@licence: Apache 2.0
#
#python /mediation/bin/insert_eri_3g4g_mos_from_cfg.py /mediation/conf/eri_cm_3g4g_parser.conf
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
                2,
                1
                );
            """, (mo,mo,))

sql="""

"""

#cur.execute( sql)


elapsed = timeit.default_timer() - start_time

print( "Execution time: {0}".format(elapsed))