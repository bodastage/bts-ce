# Generate parser config file. 
#
# This file contains a list of MOs and parameters to generate. The purpose of the config file is to ensure 
# that the parameter csv contains the same number of fields as the corresponding database tables and they 
# are in the same order. It can also be used to extract a subset of parameters from the cm data file
#
# generate_parser_config.py parser_output_dir
#
# Example: python generate_parser_config.py "data\cm\zte\gsm\parsed\in"
#
# Licence: Apache 2.0
#

import os
import sys
import csv

if len(sys.argv) != 2: 
	print("Format: {0} {1}".format( os.path.basename(__file__), "<input_directory>"))
	sys.exit()
	

folder_name=sys.argv[1] 

for file in os.listdir(folder_name):
	columns = []
	full_path = folder_name + os.path.sep + file
	
	if file == ".gitignore" : continue
		
	with open(full_path, 'r') as csvfile:
		reader = csv.reader(csvfile, quoting=csv.QUOTE_NONE)
		for row in reader:
			if len(columns) == 0:
				columns = row 
				break 

	filename = os.path.basename(file)

	mo_name = filename.replace(".csv","")
	print( "{0}:{1}".format( mo_name, ",".join(columns)) )
