# Import CM data csv files 
#   
# Usage: load_cm_data_into_db.py <schema> <csv_directory>
#
# Note:  The file names must match the table names in the schema.

import os
import sys
import csv
import subprocess
import logging



logger = logging.getLogger('load_cm_data_into_db')

# logging.basicConfig()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info("Loading CM data into database...")

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

    load_cmd = """
        pgloader -v                                   \
        --type csv                                   \
        --with truncate                              \
        --with "fields terminated by ','"            \
        --with "fields optionally enclosed by '\\"'"  \
        --with "quote identifiers"                     \
        --with "csv header"                             \
        {2} \
        postgres://bodastage:password@database/bts?tablename={0}."{1}"
    """.format(schema, mo_name, full_path)
    
#    logger.info("Loading {}.{}...".format(schema, mo_name))
    
    try:
        truncate_cmd="TRUNCATE TABLE {}.\"{}\";".format(schema, mo_name)
        copy_cmd = "\COPY {}.\"{}\" FROM '{}' CSV HEADER;".format(schema, mo_name, full_path)
        
        subprocess.call(["psql", "-U", "bodastage", "-d", "bts","-h","database" ,"-c", truncate_cmd ], env=loader_env)
        exit_code = subprocess.call(["psql", "-U", "bodastage", "-d", "bts","-h","database" ,"-c", copy_cmd ], env=loader_env)
        
        if exit_code != 0: 
            logger.error("Failed to load {}.{}".format(schema, mo_name))
            raise Exception("Failed to load {}.{}".format(schema, mo_name))
    except Exception as e:
        logger.info(str(e))
        subprocess.call(
             ["pgloader", 
              "-v", 
              "--type", "csv", 
              "--with", "truncate",
              "--with" ,"fields terminated by ','",
              "--with", """fields optionally enclosed by '\"'""",
              "--with", "quote identifiers",
              "--with", "csv header",
              full_path,
              """postgres://bodastage:password@database/bts?tablename={0}.\"{1}\"""".format(schema, mo_name)
             ], 
             env=loader_env)