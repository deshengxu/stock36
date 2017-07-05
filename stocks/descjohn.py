#!/usr/bin/env python

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from selenium.common.exceptions import NoSuchElementException
import os.path
import sys
import pandas as pd
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from selenium.webdriver.common.keys import Keys
import linecache

from stocks import helpers

def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

def process_onesymbol(downloadwait, driver, elem, symbol_current,market_current,desc_file):
    '''
    download description for each ticker and save it to file.
    :param driver:
    :param elem:
    :param symbol_current:
    :param market_current:
    :param desc_file:
    :return: 1. errorfound, yes for any error found
            2. contentfound, description tag exists?
            3. elem, for elem refresh after each query.
            4. note, status.
    '''
    try:
        elem.clear()
        elem.send_keys("{}:{}".format(market_current,symbol_current))
        elem.send_keys(Keys.RETURN)
        time.sleep(downloadwait)  #sleep more

        elem = driver.find_element_by_id('gbqfq')
        if not elem:
            return True, False, None, "Can't find tag for ticker input field!"
        try:
            company_summary_item = driver.find_element_by_class_name('companySummary')
            if not company_summary_item:
                return False, False, elem, "Can't find tag for companySummary field!" # can't find company summary
        except NoSuchElementException:
            return False, False, elem, "Can't find tag for companySummary field!"  # can't find company summary

        result = company_summary_item.text
        if (not result) or len(result) == 0:
            return False, False, elem, "text of companySummary is blank or 0" #no error found but no content found

        with open(desc_file,'w') as desc:
            desc.write(result)

        return False, True, elem, "Success"
    except:
        print("Unexpected error:", sys.exc_info()[0])
        PrintException()
        return True, False, None, sys.exc_info()[0]

def process(commandline_options):
    sourcefile = os.path.join(helpers.get_basedata_path(),commandline_options['source'])

    symbol_col = commandline_options['symbol']
    market_col = commandline_options['market']
    df_source = pd.read_csv(sourcefile)
    df_source_total = len(df_source.index)

    recordfile = None
    df_source_start_index = 0
    df_source_end_index = df_source_total
    if commandline_options['piece'] and df_source_total > commandline_options['total'] + 10:
        # if too small, we will not split
        df_source_start_index = int(df_source_total * (commandline_options['piece'] - 1) / commandline_options['total'])
        df_source_end_index = int(df_source_total * (commandline_options['piece']) / commandline_options['total'])
        print("This downloading thread will focus on index between {} and {}.".format(df_source_start_index,
                                                                                      df_source_end_index))
        # iloc slice will not include end index.
        df_source = df_source.iloc[df_source_start_index:df_source_end_index]

        recordfile = os.path.join(helpers.get_basedata_path(),
                                  "record_{}of{}_".format(commandline_options['piece'],
                                                                  commandline_options['total'])
                                  + commandline_options['source'])
    else:
        recordfile = os.path.join(helpers.get_basedata_path(), "record_" + commandline_options['source'])

    dtypedic={}
    dtypedic[symbol_col] = object
    dtypedic[market_col] = object

    df_record = helpers.get_or_create_dataframe(recordfile,
                                                [symbol_col,market_col,'processed','exportfile','note'], dtypedic)
    record_index = len(df_record.index)
    print("existing record:{}, start from:{} and end with {}".format(record_index, df_source_start_index, df_source_end_index))

    df_filtered = df_source[ ~((df_source[symbol_col].isin(df_record[symbol_col]))
                             & (df_source[market_col].isin(df_record[market_col])))]
    export_path = helpers.get_exportdata_path()

    '''initialize web driver'''
    if 'gecko' in commandline_options['driver']:
        driver = webdriver.Firefox(executable_path=commandline_options['driver'])
    elif 'chrome' in commandline_options['driver']:
        # "C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe"
        if not os.path.exists(commandline_options['driver']):
            raise IOError("Can't find chrome webdriver:{}".format(commandline_options['driver']))

        os.environ['webdriver.chrome.driver'] = commandline_options['driver']
        driver = webdriver.Chrome(commandline_options['driver'])
    else:
        driver = webdriver.PhantomJS(executable_path=commandline_options['driver'])
    driver.get("https://www.google.com/finance")
    elem = driver.find_element_by_id('gbqfq')
    if not elem:
        raise BlockingIOError("Can't find tag:gbqfq from web content. Google may have blocked this page")

    try:
        for index, row in df_filtered.iterrows():
            symbol_current = row[symbol_col]
            market_current = row[market_col]

            desc_path = os.path.join(export_path,'desc',market_current)
            if not os.path.exists(desc_path):
                os.makedirs(desc_path)
            desc_file = os.path.join(desc_path,"{}.txt".format(symbol_current))
            contentfound = True
            note = ""
            if not (os.path.exists(desc_file) and os.path.isfile(desc_file) and commandline_options['bypass']):
                errorfound, contentfound, elem, note = process_onesymbol(
                    commandline_options['downloadwait'],
                    driver,elem, symbol_current,market_current,desc_file)
                if errorfound or (not elem):
                    print("Error found, break:{}".format(desc_file))
                    break
            else:
                note = "file exist, add it to record."

            df_record.loc[record_index] = [symbol_current,market_current,contentfound,desc_file, note]
            print("{}->{}<-{},{}->{}".format(df_source_start_index,
                                             index,
                                             df_source_end_index,
                                             record_index,
                                             [symbol_current,market_current,contentfound,desc_file, note]))

            record_index += 1

    except KeyboardInterrupt:
        print("Keyboard Interrupt")

    df_record.to_csv(recordfile,index=False)
    print("total {} file processed!".format(len(df_record.index)))

def main():
    commandline_options = helpers.descjohn_commandline_parser()
    sourcefile = os.path.join(
        helpers.get_basedata_path(),
        commandline_options['source']
    )

    if not (os.path.exists(sourcefile) and os.path.isfile(sourcefile)):
        raise IOError("Can't find input csv file:{}".format(sourcefile))

    process(commandline_options)

if __name__ == '__main__':
    main()