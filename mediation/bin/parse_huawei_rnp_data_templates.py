import glob
import sys
import os
import xlrd
import csv

# List of MOs
mo_list = []

if len(sys.argv) != 4 :
    print("{} {} {}".format(sys.argv[0],'input_folder', 'output_folder', 'parameter.cfg'))

folder = sys.argv[1]
output_folder = sys.argv[2]
cfg_config = sys.argv[3] #parameter_list_xlsx.log

excel_files = glob.glob(folder+os.sep+'*')

with open(cfg_config) as f:
    data = f.readlines()
    for row in data:
        mo_and_params = row.rstrip().split(":")
        mo = mo_and_params[0]
        mo_list.append(mo)

for f in excel_files:
    workbook = xlrd.open_workbook(f, on_demand=True)
    ne_version = ""
    filename = os.path.basename(f)

    print("Processing {}".format(f))

    for sheet in workbook.sheets():
        # print("{}".format(sheet.name))

        # Get NE_VERSION
        if str(sheet.name).upper() == 'COVER':
            for row in range(sheet.nrows):
                if ne_version != "": break
                for column in range(sheet.ncols):
                    if ne_version != "": break
                    cell_value = sheet.cell(row, column).value
                    if str(cell_value).upper() == 'NE VERSION':
                        ne_version = sheet.cell(row, column+1).value
                        print("NE VERSION: {}".format(ne_version))
                        break
            continue

        # Get the list of MOs
        # The HOME sheet does not appear to exist in all cheats
        # if str(sheet.name).upper() == 'HOME':
        #     for row in range(sheet.nrows):
        #         mo_name = sheet.cell(row, 0).value
        #         if mo_name == 'MO Name': continue
        #         mo_list.append(mo_name)
        #     continue


        # Iterate through the mo worksheets
        if sheet.name in mo_list:
            csv_file = "{}.csv".format(output_folder + os.sep + sheet.name)
            csv_writer = csv.writer(open(csv_file,"w", newline=''))

            for row in range(sheet.nrows):
                # Skip the header description row i.e. row 2
                if row == 1: continue

                row_values = []

                #Add ne version
                if row == 0 :
                    row_values.append('FILENAME')
                    row_values.append('NE_VERSION')
                else:
                    row_values.append(filename)
                    row_values.append(ne_version)

                for column in range(sheet.ncols):
                    cell_value = sheet.cell(row, column).value
                    row_values.append( str(cell_value).strip('\r'))
                    # print("{},".format(sheet.cell(row, column).value),end="")
                csv_writer.writerow(row_values)

            continue

# print("{}".format(mo_list))