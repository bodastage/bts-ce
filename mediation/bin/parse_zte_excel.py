import glob
import sys
import os
import xlrd
import csv
import sys
import logging
import argparse
from collections import OrderedDict
from datetime import datetime

var_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

description = 'ZTE xlsx parser'
parser = argparse.ArgumentParser(description=description)
# group = parser.add_mutually_exclusive_group(required=True)
parser.add_argument('-i', '--input',
                    action="store", dest="input_file_or_dir",
                   metavar=('<input_file_or_dir>'),
                   help='Input file or directory',
                   required=True)

parser.add_argument('-o', '--output',
                   action="store", dest="output_dir",
                   metavar=('<output_dir>'),
                   help='Output directory', required=True)

parser.add_argument('-c', '--cfg',
                   action="store", dest="parameter_file",
                   metavar=('<parameter_file>'),
                   help='Parameter file')

args = parser.parse_args()

print(args.input_file_or_dir)
input_file_or_dir = args.input_file_or_dir
output_dir = args.output_dir

logger = logging.getLogger(sys.argv[0])

# logging.basicConfig()
handler = logging.StreamHandler()
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)




raw_file_list = []

if os.path.isdir(input_file_or_dir):
    raw_file_list = glob.glob(input_file_or_dir + os.sep + '*')

if os.path.isfile(input_file_or_dir):
    raw_file_list.append(input_file_or_dir)

mo_param_list = {}

# logger.info("raw_file_list: {}".format(raw_file_list))

sheets_to_skip = ['TemplateInfo', 'Index']

mo_csvwriters = {}


if args.parameter_file is None:
    for f in raw_file_list:
        logger.info("Collecting managed objects and parameters from {}....".format(os.path.basename(f)))

        xl_workbook = xlrd.open_workbook(f, on_demand=True)
        sheet_names = xl_workbook.sheet_names()

        # Collect MOs
        for m in sheet_names:

            # Skip some sheets
            if m in sheets_to_skip: continue

            if m not in mo_param_list:
                mo_param_list[m] = []
                # filename = output_dir + os.sep + m + ".csv"
                # csvfile = open(filename, 'w', newline='')
                # mo_csvwriters[m] = csv.writer(csvfile)

                nrm_worksheet = xl_workbook.sheet_by_name(m)

                for row in range(nrm_worksheet.nrows):
                    if row > 0 : break

                    for column in range(nrm_worksheet.ncols):
                        cell_value = nrm_worksheet.cell(row, column).value
                        if cell_value not in mo_param_list[m]:
                            mo_param_list[m].append(cell_value)

#
else:
    with open(args.parameter_file)as fp:
        for line in fp:
            mo, params = line.strip().split(":")
            mo_param_list[mo] = params.split(",")



logger.info("Collecting parameter values...")

# This will contain MOs whose headers have been added to the csv files
header_added = []

print()
# Parse file
for f in raw_file_list:
    xl_workbook = xlrd.open_workbook(f, on_demand=True)
    sheet_names = xl_workbook.sheet_names()

    for mo in sheet_names:
        if mo in sheets_to_skip: continue

        # Skip parameters not in the cfg
        if mo not in mo_param_list: continue

        nrm_worksheet = xl_workbook.sheet_by_name(mo)

        params = []
        params_data = OrderedDict()
        for row in range(nrm_worksheet.nrows):

            # Get headers
            if row == 0:
                if mo not in header_added:

                    # Create file
                    filename = output_dir + os.sep + mo + ".csv"
                    csvfile = open(filename, 'w', newline='')
                    mo_csvwriters[mo] = csv.writer(csvfile)

                    # Add header
                    mo_csvwriters[mo].writerow( ["FILENAME", "DATETIME"] + mo_param_list[mo])
                else:
                    header_added.append(mo)

                for c in range(nrm_worksheet.ncols):
                    m = nrm_worksheet.cell(row, c).value
                    params.append(m)

            if row in [0, 1, 2, 3, 4]: continue

            for column in range(nrm_worksheet.ncols):
                cell_value = nrm_worksheet.cell(row, column).value
                pname = params[column]
                params_data[pname] = cell_value

            row_values =[]
            for p in mo_param_list[mo]:

                if p in params_data:
                    row_values.append(params_data[p])
                else:
                    row_values.append("")

            # print(row_values)
            try:
                f_name = os.path.basename(f)
                mo_csvwriters[mo].writerow( [f_name, var_datetime ] + row_values)
            except Exception as e:
                logger.error("mo:{} row_values:{}".format(mo, row_values))
                pass

logger.info("Parsing done.")