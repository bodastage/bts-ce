# Generate table creation sql
# 
# generate_table_sql.py schema parser_output_dir
# Assumes that <schema>_hist exists
#
# Example: python generate_table_sql.py zte_cm_2g "data\cm\zte\gsm\parsed\in"
#
# Change log:
# 23/11/2017 Make columns below text fields for Ericsson 2G cnaiv2 dumps
#	INTERNAL_CELL.UHPRIOTHR, 
#	INTERNAL_CELL.UQRXLEVMIN
#	INTERNAL_CELL.ULPRIOTHR, 
#	INTERNAL_CELL.UMFI_ACTIVE, 
#	INTERNAL_CELL. COVERAGEU
#	INTERNAL_CELL.UMFI_IDLE, 
#	MSC.LAI and 
#	BSC.CAPACITYLOCKS 
#
# Licence: Apache 2.0
#

import os
import sys
import csv

if len(sys.argv) != 3: 
	print("Format: {0} {1} {2}".format( os.path.basename(__file__), "<schema>", "<input_directory>"))
	sys.exit()
	
schema= sys.argv[1] 

folder_name=sys.argv[2] 

print( "-- psql -U bodastage -d bodastage -a -f {0}.sql".format(schema))
print( "-- \i {0}_tables.sql".format(schema))
print ("")

text_fields = ["LAI","CAPACITYLOCKS","UMFI_IDLE","ULPRIOTHR","UQRXLEVMIN","COVERAGEU","UHPRIOTHR", "reservedBy",
				"certificateContent_publicKey", "sectorCarrierRef", "trustedCertificates", "consistsOf","consistsOf","listOfNe",
				"equipmentClockPriorityTable","rfBranchRef", "syncRiPortCandidate", "transceiverRef","vsdatamulticastantennabranch",
				"associatedRadioNodes","ethernetPortRef","staticRoutes_ipAddress", "staticRoutes_networkMask",
				"staticRoutes_nextHopIpAddr","staticRoutes_redistribute","ipInterfaceMoRef","ipAccessHostRef",
				"physicalPortList","additionalText","candNeighborRel_enbId", "vlanRef","trDeviceRef","port","iubLinkUtranCell",
				"manages","iubLinkUtranCell", "FREQLST"]

for file in os.listdir(folder_name):
	columns = []
	sample_row = []
	mo_table_sql=""
	mo_table_sql_hist=""
	
	full_path = folder_name + os.path.sep + file
	
	if file == ".gitignore" : continue
		
	with open(full_path, 'r') as csvfile:
		reader = csv.reader(csvfile, quoting=csv.QUOTE_NONE)
		for row in reader:
			if len(columns) == 0:
				columns = row 
				continue 
			if len(sample_row) == 0:
				sample_row = row
				break

	filename = os.path.basename(file)

	mo_name = filename.replace(".csv","")
	mo_table_sql = "CREATE TABLE IF NOT EXISTS {0}.{1}".format(schema,mo_name)
	mo_table_sql += "("
	
	mo_table_sql_hist = "CREATE TABLE IF NOT EXISTS {0}_hist.{1}".format(schema,mo_name)
	mo_table_sql_hist += "("

	length = len(columns)
	for idx in range(length):
		column = columns[idx]
		value  = sample_row[idx]
			
		comma = ","
		if idx == length-1 :
			comma = ""
		
		#print("column: {0} value: {1} len(value):".format(column,value, len(value)))
		#print(sample_row)
		
		try:
			#@TODO: Handle datetime fields separately 
			if column == "varDateTime":
				mo_table_sql += " \"{0}\" TIMESTAMP {1}".format(column,comma)
				mo_table_sql_hist += " \"{0}\" TIMESTAMP {1}".format(column,comma)
			#LAI,UMFI_ACTIVE,and CAPACITYLOCKS are from Ericsson CNAIv2 dumps
			elif len(value) > 250  or  column in text_fields or column[-3:].lower() == 'ref' :
				mo_table_sql += " \"{0}\" text {1}".format(column,comma)
				mo_table_sql_hist += " \"{0}\" text {1}".format(column,comma)
			elif "_BIT" in column:
				mo_table_sql += " \"{0}\" char(5) {1}".format(column,comma)
				mo_table_sql_hist += " \"{0}\" char(5) {1}".format(column,comma)
			else:
				mo_table_sql += " \"{0}\" char(250) {1}".format(column,comma)
				mo_table_sql_hist += " \"{0}\" char(250) {1}".format(column,comma)
		except Exception as ex:
				mo_table_sql += " \"{0}\" text {1}".format(column,comma)
				mo_table_sql_hist += " \"{0}\" text {1}".format(column,comma)
		
	mo_table_sql += ");"
	mo_table_sql += "\n"
	mo_table_sql_hist += ");"
	mo_table_sql_hist += "\n"
	
	print( mo_table_sql)
	print( mo_table_sql_hist)