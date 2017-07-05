#!/usr/bin/env python

import csv

import time
import argparse

import os.path
import sys

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from stocks import helpers

def clean_desc_record(commandline_option):
    exportfile = os.path.join(helpers.get_exportdata_path(), commandline_option['target'])
    csv_header = ['market', 'symbol', 'filename']

    with open(exportfile, "a") as csvfile:
        csvfile.write(",".join(csv_header) + "\n")
        index = 0
        for dir_name, subdir_list, file_list in os.walk(commandline_option['source']):
            for file_name in file_list:
                csv_fields = []
                market = os.path.basename(os.path.normpath(dir_name))
                csv_fields.append(market)  # market name
                csv_fields.append(os.path.splitext(file_name)[0])   #symbol
                csv_fields.append("desc/" + market + "/" + file_name)
                print("{}->{}".format(index, csv_fields))
                csvfile.write(",".join(csv_fields) + "\n")

                index += 1

def clean_price_record(commandline_option):
    exportfile = os.path.join(helpers.get_exportdata_path(),commandline_option['target'])
    csv_header=['market','symbol','yahoomarket','filename']

    with open(exportfile,"a") as csvfile:
        csvfile.write(",".join(csv_header) + "\n")
        index=0
        for dir_name, subdir_list, file_list in os.walk(commandline_option['source']):
            for file_name in file_list:
                csv_fields = []
                market = os.path.basename(os.path.normpath(dir_name))
                csv_fields.append(market) #market name
                name_yahoomarket = os.path.splitext(file_name)[0].split(".")
                if len(name_yahoomarket) == 1:
                    name_yahoomarket.append(market)

                csv_fields.extend(name_yahoomarket)
                csv_fields.append("price/" + market + "/" + file_name)
                print("{}->{}".format(index, csv_fields))
                csvfile.write(",".join(csv_fields) + "\n")

                index += 1

def main():
    commandline_option = get_command()

    if not commandline_option['yahoo']:
        clean_desc_record(commandline_option)
    else:
        clean_price_record(commandline_option)

def get_command():
    parser = argparse.ArgumentParser(description="record cleaning tool")
    parser.add_argument('-source', action="store", dest="source", required=False,
                        default=".",
                        help="source folder")
    parser.add_argument('-target', action="store", dest="target", required=False,
                        default="record.csv",
                        help="export file in export data folder")
    parser.add_argument('-yahoo', action="store_true", dest="yahoo", required=False,
                        default=False,
                        help="whether includes yahoo market name column")  # control broken download

    args = parser.parse_args()

    return_options = {}
    return_options['source'] = args.source
    return_options['target'] = args.target
    return_options['yahoo'] = args.yahoo

    if not (os.path.exists(args.source) and os.path.isdir(args.source)):
        raise IOError("this folder doesn't exist:{}".format(args.source))

    return return_options

if __name__ == '__main__':
    main()