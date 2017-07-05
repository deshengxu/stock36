# -*- coding: utf-8 -*-
#!/usr/bin/env python

import urllib.parse
import urllib.request
from datetime import datetime
import pytz
import pandas as pd

from bs4 import BeautifulSoup

from pandas_datareader import DataReader
import pandas_datareader.data as web

import os
#from . import helpers


SITE = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
START = datetime(2016, 1, 1, 0, 0, 0, 0, pytz.utc)
END = datetime.today().utcnow()


def scrape_list(site):
    hdr = {'User-Agent': 'Mozilla/5.0'}
    req = urllib.request.Request(SITE, data=None, headers=hdr)
    #sector_tickers = {}
    with urllib.request.urlopen(req) as response:
        page = response.read()
        soup = BeautifulSoup(page, "html.parser")

        table = soup.find('table', {'class': 'wikitable sortable'})
        sector_tickers = dict()
        for row in table.findAll('tr'):
            col = row.findAll('td')
            if len(col) > 0:
                sector = str(col[3].string.strip()).lower().replace(' ', '_')
                ticker = str(col[0].string.strip())
                if sector not in sector_tickers:
                    sector_tickers[sector] = list()
                sector_tickers[sector].append(ticker)

        print(sector_tickers)

        return sector_tickers
    return None

def download_stock_csv(sector_tickers, start, end):
    for sector, tickers in sector_tickers.items():
        print('Downloading data from Yahoo for %s sector' % sector)
        for ticker in tickers:
            print("Downloading ..."+ticker)
            mydata = web.DataReader(ticker,'yahoo', start, end)
            mydata.to_csv("./"+ticker+".csv")
            print(ticker+".csv is done!")

    print('Finished downloading data')

def download_ohlc(sector_tickers, start, end):
    sector_ohlc = {}
    for sector, tickers in sector_tickers.items():
        print('Downloading data from Yahoo for %s sector' % sector)
        #key api to download data from yahoo for tickers.
        data = web.DataReader(tickers, 'yahoo', start, end)
        sector_ohlc[sector] = data
    print('Finished downloading data')
    return sector_ohlc

def export_to_csv(sector_ohlc, rootPath="."):
    '''
    sector_ohlc is a directionary
    key: panel
    :param sector_ohlc:
    :param rootPath:
    :return:
    '''
    for sector, panel_data in sector_ohlc.iteritems():
        folder_path = os.path.join(rootPath,sector)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print("Folder has been created:" + folder_path)

        for key in panel_data.minor_axis:
            key_file = os.path.join(folder_path, key + ".csv")
            (panel_data.minor_xs(key)).to_csv(key_file)
            print("File has been created:" + key_file)


def store_HDF5(sector_ohlc, path):
    with pd.get_store(path) as store:
        for sector, ohlc in sector_ohlc.iteritems():
            store[sector] = ohlc


def get_snp500():

    sector_tickers = scrape_list(SITE)
    #print(sector_tickers)
    sector_ohlc = download_ohlc(sector_tickers, START, END)
    #download_stock_csv(sector_tickers, START, END)
    #print(sector_ohlc)
    #store_HDF5(sector_ohlc, 'snp500.h5')
    export_to_csv(sector_ohlc, rootPath="./exportdata/")


if __name__ == '__main__':
    get_snp500()