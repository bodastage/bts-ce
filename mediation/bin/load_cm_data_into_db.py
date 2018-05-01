# Import CM data csv files 
#   
# Usage: load_cm_data_into_db.py <schema> <csv_directory>
#
#Note:  The file names must match the table names in the schema.

import os
import sys
import csv
import subprocess

if len(sys.argv) != 3: 
	print("Format: {0} {1} {2}".format( os.path.basename(__file__), "<schema>", "<csv_directory>"))
	sys.exit()
	
schema= sys.argv[1] 

csv_folder=sys.argv[2] 

# Loader environment variables
loader_env = {}

loader_env["PGPASSWORD"] = 'password'

for file in os.listdir(csv_folder):
	filename = os.path.basename(file)
	mo_name = filename.replace(".csv","")
	full_path = csv_folder + os.path.sep + file
	
	truncate_cmd="TRUNCATE TABLE {}.\"{}\";".format(schema, mo_name)
	copy_cmd = "\COPY {}.\"{}\" FROM '{}' CSV HEADER;".format(schema, mo_name, full_path)
	
	print(truncate_cmd)
	print(copy_cmd)
	print("")
	
	subprocess.call(["psql", "-U", "bodastage", "-d", "bts","-h","database" ,"-c", truncate_cmd ], env=loader_env)
	subprocess.call(["psql", "-U", "bodastage", "-d", "bts","-h","database" ,"-c", copy_cmd ], env=loader_env)