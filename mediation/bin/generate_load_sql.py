# Generate sql file to load data into the db for PostgreSQL
# 
# generate_load_sql.py vendor technology parser_output_dir
#
# Example: python generate_load_sql.py zte_cm_2g "C:\Users\Emmanuel\Documents\Bodastage\Boda_CM_Parsers\zte\out\gsm"
#
import os
import sys
import csv

#vendor 
schema=sys.argv[1] 

#output folder
folder_name=sys.argv[2] 

print( "-- psql -U bodastage -d bodastage -a -f {0}.load.sql".format(schema) )
print( "-- \i {0}_loader.sql".format(schema) )
print("")

for file in os.listdir(folder_name):

	colunms=[]
	full_path = folder_name + os.path.sep + file

	
	filename = os.path.basename(file)

	with open(full_path, 'r') as csvfile:
		reader = csv.reader(csvfile, quoting=csv.QUOTE_NONE)
		for row in reader:
			colunms=row
			break
		
	#column_list = "\"" + "\",\"".join(colunms) + "\"";
	column_list = ",".join(colunms);
	
	#if filename == '.gitignore': continue

	mo_name = filename.replace(".csv","")
	
	#Comment in file 
	print("")
	print("-- {0}".format(mo_name))
	print("-- ---------------------------")
	
	#@TODO: Back data in history schema 
	#history_schema=schema+"_history"
	#print( "\COPY {0}.{1}({2}) FROM '{3}.{1}'".format(schema,mo_name,history_schema))
	
	#truncate table 
	print( "TRUNCATE TABLE {0}.{1};".format(schema,mo_name,column_list,full_path))
	
	#Copy from file to table 
	#print( "\COPY {0}.{1}({2}) FROM '{3}' CSV HEADER;".format(schema,mo_name,column_list,full_path)) #Specifiy the columns
	print( "\COPY {0}.{1} FROM '{2}' CSV HEADER;".format(schema,mo_name,full_path)) #This assumes the columns in the csv file match the table columns