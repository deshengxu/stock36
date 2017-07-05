import argparse
import re
import os

def command_line_parser():
    parser = argparse.ArgumentParser(description="Stock data downloader.")
    parser.add_argument('-log', action="store", dest="log", required=True, default="./nohup.out",
                        help="Log file which includes history record.")
    parser.add_argument('-record', action="store", dest="record", required=True,
                        help="price retrive record file")

    args = parser.parse_args()

    if os.path.exists(args.log) and os.path.exists(args.record) and \
        os.path.isfile(args.log) and os.path.isfile(args.record):
        command_options={}
        command_options['log'] = args.log
        command_options['record'] = args.record

        return command_options
    else:
        raise IOError("Can't find both file.")

def get_lines(files):
    for f in files:
        for line in f:
            yield line

def fixrecords(logfile, recordfile):
    with open(logfile,"r") as log, open(recordfile,"a") as record:
        for line in get_lines([log]):
            m = re.search(">\[(.*?)\]", line)
            if m:
                fields = m.group(1).split(",")
                newfields = []
                for field in fields:
                    newfield = field.strip()
                    if len(newfield)>2 and (newfield[0]=='"' or newfield[-1] == "'"):
                        newfield = newfield[1:-1]
                    newfields.append(newfield)

                fieldsstr= ",".join(newfields)
                record.write(fieldsstr+"\n")

def main():
    command_options=command_line_parser()
    fixrecords(command_options['log'], command_options['record'])

if __name__ == '__main__':
    main()


