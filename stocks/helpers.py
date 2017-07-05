# -*- coding: utf-8 -*-
#!/usr/bin/env python
from selenium import webdriver
import argparse
import os
import os.path
import sys
from datetime import datetime

import pandas as pd
import json
from pprint import pprint
import glob
import linecache
import logging

def get_driver(driver_option = None):
    driver = None
    if not driver_option:
        driver_option = "C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe"

    if 'gecko' in driver_option:
        firefoxProfile = webdriver.FirefoxProfile()
        firefoxProfile.set_preference('permissions.default.stylesheet', 2)
        firefoxProfile.set_preference('permissions.default.image', 2)
        firefoxProfile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
        firefoxProfile.set_preference("http.response.timeout", 10)
        firefoxProfile.set_preference("dom.max_script_run_time", 10)
        # add profile for better timeout control.
        driver = webdriver.Firefox(executable_path=driver_option,
                                   firefox_profile=firefoxProfile)
    elif 'chrome' in driver_option:
        # "C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe"
        if not os.path.exists(driver_option):
            raise IOError("Can't find chrome webdriver:{}".format(driver_option))

        os.environ['webdriver.chrome.driver'] = driver_option
        driver = webdriver.Chrome(driver_option)
    else:
        driver = webdriver.PhantomJS(executable_path=driver_option)

    return driver

def convert_file(inputfile,outputfile):
    '''
    convert file in latin_1 encoding to utf-8 encoding.
    :param inputfile:
    :param outputfile:
    :return:
    '''
    with open(inputfile,'r', encoding='latin_1') as fr, open(outputfile,'w',encoding='utf-8') as fw:
        fw.write(fr.read())

def get_or_create_dataframe(existingfile, columnslist, dtypedict=None):
    if os.path.exists(existingfile) and os.path.isfile(existingfile):
        return pd.read_csv(existingfile, dtype=dtypedict)
    else:
        return pd.DataFrame(columns=columnslist)

def get_default_cols_list():
    return ["Symbol","Name","LastSale","MarketCap","IPOyear","Sector","industry","Summary Quote","market"]

def get_basedata_path():
    return get_subfolder_path('basedata')

def get_exportdata_path():
    return get_subfolder_path('exportdata')

def get_subfolder_path(folder_name):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    subfolder_path = os.path.join(dir_path, "../{}/".format(folder_name))

    if os.path.exists(subfolder_path) and os.path.isdir(subfolder_path):
        return subfolder_path

    raise IOError("can't find {} folder!".format(folder_name))

def get_keyword_file(company_listfile):
    return os.path.join(os.path.dirname(company_listfile),"keywords.csv")

def get_company_list_file(market):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    json_file = os.path.join(dir_path,"../basedata/", "marketlist.json")

    if not (os.path.exists(json_file) and os.path.isfile(json_file)):
        return get_minimal_company_list_file(market)

    with open(json_file, 'r') as json_source:
        company_list = json.load(json_source)

    try:
        list_file = company_list[market]['ListFile']
    except:
        pprint(company_list)
        raise ValueError("Can't find proper market in configuration file.")

    if not os.path.isabs(list_file):
        list_file = os.path.join(dir_path, '../basedata/', list_file)

    if not (os.path.exists(list_file) and os.path.isfile(list_file)):
        raise ValueError("Can't find company list file:{}".format(list_file))

    print(list_file)
    return list_file


def get_minimal_company_list_file(market):
    markets = ['NASDAQ', 'NYSE', 'AMEX']
    if not market in markets:
        raise ValueError("market should be one of:{}".format(markets))

    market_list = {
        "NASDAQ": "companylist-NASDAQ.csv",
        "AMEX": "companylist-AMEX.csv",
        "NYSE": "companylist-NYSE.csv"
    }
    dir_path = os.path.dirname(os.path.realpath(__file__))
    companylist_file = os.path.join(dir_path, "../basedata/", market_list[market])

    if not (os.path.exists(companylist_file) and os.path.isfile(companylist_file)):
        raise ValueError("Can't find company list file:{}".format(companylist_file))

    return companylist_file


def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

def get_logger_level(loglevel):
    levels = {'CRITICAL': logging.CRITICAL,
              'ERROR': logging.ERROR,
              'WARNING': logging.WARNING,
              'INFO': logging.INFO,
              'DEBUG': logging.DEBUG
              }

    return levels.get(loglevel, logging.INFO)

def pricejohn_commandline_parser():
    '''
    parser command line options for descjohn.py and return with a dictionary
    :return:
    '''

    parser = argparse.ArgumentParser(description="John's ticker description tool command line parser")
    parser.add_argument('-driver', action="store", dest="driver", required=False,
                        default='/Users/desheng/builds/phantomjs/bin/phantomjs',
                        help="path to gecko driver or phantomjs")
    parser.add_argument('-source', action="store", dest="source", required=False,
                        default="ticker_all_verified_final.csv",
                        help="file which includes all verified ticker, relative to basedata folder.")
    parser.add_argument('-symbol', action="store", dest="symbol", required=False,
                        default="symbol",
                        help="symbol field column header")
    parser.add_argument('-market', action="store", dest="market", required=False,
                        default="market",
                        help="market field column header.")
    parser.add_argument('-target', action="store", dest="target", required=False,
                        default="yahoo",
                        help="indicate yahoo or google, by default, it will be yahoo.")
    parser.add_argument('-total', action="store", dest="total", required=False, default=5, type=int,
                        help='total pieces to split.')
    parser.add_argument('-piece', action="store", dest="piece", required=False, default=-1, type=int,
                        help='value should be between 1 and total, -1 means all.')
    parser.add_argument('-dropwait', action="store", dest="dropwait", required=False, default=20, type=int,
                        help='time to wait for dropdown list')
    parser.add_argument('-downloadwait', action="store", dest="downloadwait", required=False, default=5, type=int,
                        help='time to wait for download href link.')
    parser.add_argument('-bypass', action="store_true", dest="bypass", required=False,
                        default=False,
                        help="if desc exists, then bypass it")  #control broken download
    parser.add_argument('-startdate', action="store", dest="startdate", required=False,
                        default="20000101",
                        help="start date to extract stock from google.")  # control broken download
    parser.add_argument('-loglevel', action="store", dest="loglevel", required=False,
                        default="20000101",
                        help="logger level: INFO, DEBUG etc")  # control broken download
    parser.add_argument('-entry_mode', action="store", dest="entry_mode", required=False,
                        default="shortpath",    #fullpath, shortpath
                        help="directly access to history price link or start from google finance")

    args = parser.parse_args()

    return_options = {}
    return_options["driver"] = args.driver
    return_options['source'] = args.source
    return_options['symbol'] = args.symbol
    return_options['market'] = args.market
    return_options['target'] = args.target
    return_options['dropwait'] = args.dropwait  #need validate
    return_options['downloadwait'] = args.downloadwait #need validate
    return_options['bypass'] = args.bypass
    return_options['startdate'] = args.startdate
    return_options['loglevel'] = args.loglevel
    return_options['entry_mode'] = args.entry_mode

    if not (1 < args.total < 100):
        raise IndexError("Total should be bigger than 2 and less than 100, but now:{}".format(args.total))

    if not (args.piece == -1 or 0 < args.piece <= args.total):
        raise IndexError(
            "Piece parameter should be either -1 (all) or between 1 and total, but now:{}".format(args.piece))

    return_options['total'] = args.total
    if args.piece == -1:
        return_options['piece'] = None
    else:
        return_options['piece'] = args.piece

    return return_options

def descjohn_commandline_parser():
    '''
    parser command line options for descjohn.py and return with a dictionary
    :return:
    '''

    parser = argparse.ArgumentParser(description="John's ticker description tool command line parser")
    parser.add_argument('-driver', action="store", dest="driver", required=False,
                        default='/Users/desheng/builds/phantomjs/bin/phantomjs',
                        help="path to gecko driver or phantomjs")
    parser.add_argument('-source', action="store", dest="source", required=False,
                        default="ticker_all_verified_final.csv",
                        help="file which includes all verified ticker, relative to basedata folder.")
    parser.add_argument('-symbol', action="store", dest="symbol", required=False,
                        default="symbol",
                        help="symbol field column header")
    parser.add_argument('-market', action="store", dest="market", required=False,
                        default="market",
                        help="market field column header.")
    parser.add_argument('-bypass', action="store_true", dest="bypass", required=False,
                        default=False,
                        help="if desc exists, then bypass it")  #control broken download
    parser.add_argument('-total', action="store", dest="total", required=False, default=5, type=int,
                        help='total pieces to split.')
    parser.add_argument('-piece', action="store", dest="piece", required=False, default=-1, type=int,
                        help='value should be between 1 and total, -1 means all.')
    parser.add_argument('-downloadwait', action="store", dest="downloadwait", required=False, default=15, type=int,
                        help='time to wait for download href link.')

    args = parser.parse_args()

    return_options = {}
    return_options["driver"] = args.driver
    return_options['source'] = args.source
    return_options['symbol'] = args.symbol
    return_options['market'] = args.market
    return_options['bypass'] = args.bypass
    return_options['total'] = args.total
    return_options['piece'] = args.piece
    return_options['downloadwait'] = args.downloadwait

    return return_options

def cleanjohn_commandline_parser():
    parser = argparse.ArgumentParser(description="John's ticker clean tool command line parser")
    parser.add_argument('-driver', action="store", dest="driver", required=False,
                        default='/Users/desheng/builds/phantomjs/bin/phantomjs',
                        help="path to gecko driver or phantomjs")
    parser.add_argument('-source', action="store", dest="source", required=False,
                        default="ticker_all.csv",
                        help="file which includes all original ticker, relative to basedata folder.")
    parser.add_argument('-record', action="store", dest="record", required=False,
                        default="ticker_all_records.csv",
                        help="file which includes checking record for all original ticker, relative to basedata folder.")
    parser.add_argument('-verified', action="store", dest="verified", required=False,
                        default="ticker_all_verified.csv",
                        help="file which includes verfied record for all original ticker, relative to basedata folder.")
    parser.add_argument('-notfound', action="store", dest="notfound", required=False,
                        default="ticker_all_notfound.csv",
                        help="file which includes not-found record for all original ticker, relative to basedata folder.")

    args = parser.parse_args()
    sourcefile = os.path.join(get_basedata_path(),args.source)
    if not (os.path.exists(sourcefile) and os.path.isfile(sourcefile)):
        raise IOError("Source file not found:{}".format(args.source))

    if not (os.path.exists(args.driver) and os.path.isfile(args.driver)):
        raise IOError("Can't find geco driver:{}".format(args.driver))

    recordfile = os.path.join(get_basedata_path(),args.record)
    verifiedfile = os.path.join(get_basedata_path(),args.verified)
    notfoundfile = os.path.join(get_basedata_path(),args.notfound)

    return args.driver,sourcefile,recordfile,verifiedfile,notfoundfile


def command_line_parser():
    parser = argparse.ArgumentParser(description="Stock data downloader.")
    parser.add_argument('-market', action="store", dest="market", required=True,
                        help="Market to download, it should be one of NASDAQ, NYSE, AMEX")
    parser.add_argument('-root', action="store", dest="root", required=False,
                        default="./exportdata",
                        help="root folder to store downloaded data, for example . or ./exportdata/ etc")
    parser.add_argument('-max', action="store", dest="max", required=False,
                        default="0", type=int,
                        help="max tickers to download, for example: 10 or 100 or 1000.")
    parser.add_argument('-sector', action="store", dest="sector", required=False, default=None,
                        help='Sector to filter, for example: "Finance,Consumer Services"')
    parser.add_argument('-industry', action="store", dest="industry", required=False, default=None,
                        help='industry to filter, for example: "Property-Casualty Insurers,Finance/Investors Services"')
    parser.add_argument('-additional', action="store", dest="additional", required=False, default=None,
                        help='additional col to indicate selection.')
    parser.add_argument('-start', action="store", dest="start", required=False, default=None,
                        help='start date for query, format:yyyymmdd 20100101.')
    parser.add_argument('-end', action="store", dest="end", required=False, default=None,
                        help='end date for query, format:yyyymmdd 20100101.')

    args = parser.parse_args()

    market = (args.market).upper()

    companylist_file = get_company_list_file(market)

    if not os.path.exists(args.root):
        try:
            os.makedirs(args.root)
        except:
            raise ValueError("Unexpected error:", sys.exc_info()[0])

    sectorfilter = None
    if args.sector:
        sectorfilter = (args.sector).replace('"','')

    industryfilter = None
    if args.industry:
        industryfilter = (args.industry).replace('"','')

    additionalfilter = None
    if args.additional:
        additionalfilter = (args.additional).replace('"','')
    
    startdate=None
    if args.start:
        try:
            startdate= datetime.strptime(args.start, '%Y%m%d')
        except:
            raise ValueError("{} format error.".format(args.start))
    
    enddate=None
    if args.end:
        try:
            enddate= datetime.strptime(args.end, '%Y%m%d')
        except:
            raise ValueError('{} format error.'.format(args.end))

    return companylist_file, args.root, args.max, args.market, sectorfilter,industryfilter,additionalfilter,startdate,enddate


class StockOption:
    def __init__(self):
        self.com_list = None
        self.root = None
        self.max = 0
        self.market = None
        self.startdate=None
        self.enddate=None

    def init_by_sec(self, companylistfile, root, max, market, sectorfilter, industryfilter, add_filter_col,startdate,enddate ):
        '''

        :param companylistfile: format of company list downloaded from sec
        :param root: root folder to store data. default is ./exportdata
        :param max: max tickers to be proccessed
        :param market: market name, NASDAP, AMEX, NYSE etc
        :param sectorfilter: string with common to indicate how many sector will be downloaded
        :param industryfilter: string with common to indicate how many industry will be downloaded
        :param add_filter_col: additional column with value "Y"
        '''
        com_list = pd.read_csv(companylistfile)
        headers = list(com_list)
        required_cols = ['Symbol', 'IPOyear', 'Sector', 'industry']

        if add_filter_col:
            required_cols.append(add_filter_col)

        if not self.check_header(required_cols, headers):
            raise ValueError("{},Some header missed in file:{}".format(required_cols, companylistfile))

        if not os.path.exists(root):
            try:
                os.makedirs(root)
            except:
                raise ValueError("Unexpected error:", sys.exc_info()[0])

        if not os.path.isdir(root):
            raise ValueError("{} should be folder.".format(root))

        if sectorfilter:
            sector_filter_list = sectorfilter.split(",")
            com_list = com_list.loc[com_list['Sector'].isin(sector_filter_list)]
        if industryfilter:
            industry_filter_list = industryfilter.split(",")
            com_list = com_list.loc[com_list['industry'].isin(industry_filter_list)]
        if add_filter_col:
            com_list = com_list.loc[com_list[add_filter_col].isin(['Y', 'y', 'T', 'TRUE','True', 'true', 'YES', 'Yes', 'yes'])]
        if max>0:
            com_list = com_list.head(max)
        
        self.startdate=startdate
        self.enddate=enddate
        #com_list.loc[com_list['Sector'] == 'n/a', 'Sector'] = 'NotAvailable'
        #com_list.loc[com_list['industry'] == 'n/a', 'industry'] = 'NotAvailable'
        #test.loc[test['Sector']=='n/a','industry']=test['industry'].str.lower()+"hahaha"
        #above code is a way to update value by selection
        com_list.loc[com_list['IPOyear'] == 'n/a', 'IPOyear'] = '2000'
        com_list['industry'].replace(['&','\/','\\\\',':','n_a','n\/a'],
                                     ['_','_','_','_','NotAvailable','NotAvailable'],
                                     regex=True, inplace=True)
        com_list['Sector'].replace(['&', '\/', '\\\\', ':', 'n_a', 'n\/a'],
                                     ['_', '_', '_', '_', 'NotAvailable', 'NotAvailable'],
                                     regex=True, inplace=True)

        self.com_list = com_list
        self.root = root
        self.max = max
        self.market = market

    def get_total_list(self):
        if self.com_list:
            return len(self.com_list)
        else:
            raise ValueError("Data hasn't bee initialized yet")

    def get_processed_file(self):
        if self.root:
            return os.path.join(self.root, "processed_tickets.csv")

    def check_header(self, required_cols, total_cols):
        '''
        make sure total_cols includes all required_cols
        :param required_cols:
        :param total_cols:
        :return:
        '''
        return len(required_cols) == len(set(required_cols) & set(total_cols))
