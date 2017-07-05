# -*- coding: utf-8 -*-
#!/usr/bin/env python

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from bs4 import BeautifulSoup

import os.path
import sys
from datetime import datetime
import pandas as pd
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from stocks.helpers import StockOption
from stocks import helpers


def verify_all(driver, source, record, verified, notfound):
    #driver = webdriver.Firefox(executable_path=driver)
    driver = webdriver.PhantomJS(executable_path=driver)
    driver.get("https://www.google.com/finance")
    elem = driver.find_element_by_id('gbqfq')
    if not elem:
        raise BlockingIOError("Can't find tag:gbqfq from web content. Google may have blocked this page")

    df_source = pd.read_csv(source)

    #in record file, each symbol may be:
    #Results: how many have been found, doesn't matter name matches or not. for example: IBEX -> IB, IBEX
    #FullyMatches: how many result are exactly matched with original string.
    cols_record = ["symbol","Results", "FullyMatches"]
    cols_verified = ["symbol","market","name","origin_name","origin_index"]
    cols_notfound = ["symbol", "reason", "origin_name","origin_index"]
    df_record = helpers.get_or_create_dataframe(record,cols_record)
    df_verified = helpers.get_or_create_dataframe(verified,cols_verified)
    df_notfound = helpers.get_or_create_dataframe(notfound,cols_notfound)

    try:
        df_record_index = len(df_record.index)
        df_verified_index = len(df_verified.index)
        df_notfound_index = len(df_notfound.index)

        df_source = df_source[df_source['oftic'].notnull()]
        df_source_unique = df_source[df_source.groupby(['oftic'])['sdates'].transform(min) == df_source['sdates']]
        df_source_unique = df_source_unique[~df_source_unique['oftic'].isin(df_record['symbol'])]

        last_length = 1
        for index, row in df_source_unique.iterrows():
            ticker = row['oftic']
            #searchstr = '\b' * last_length + ticker
            last_length = len(ticker)
            #elem.send_keys(searchstr)
            elem.clear()
            elem.send_keys(ticker)
            time.sleep(1)

            content = driver.page_source
            soup = BeautifulSoup(content, "html.parser")
            result = parse_content(soup)

            if len(result) == 0:
                df_record.loc[df_record_index] = [ticker,0,0]
                df_notfound.loc[df_notfound_index] = [ticker,"Just not found", ticker, index]
                df_record_index += 1
                df_notfound_index += 1
                print("{} {} not found".format(index, ticker))

            else:
                fully_match_count = 0
                for key, option in result.items():
                    if option['symbol'] == ticker:
                        fully_match_count += 1
                        df_verified.loc[df_verified_index] = [option['symbol'], option['market'], option['name'],
                                                              ticker, index]
                        df_verified_index += 1
                        print("{} {}->{}\t{}\t{}".format(index, ticker, option['symbol'], option['market'],
                                                         option['name']))

                df_record.loc[df_record_index] = [ticker, len(result), fully_match_count]
                df_record_index += 1
    except KeyboardInterrupt:
        print("Keyboard Interrupt!")

    df_record.to_csv(record, index=False)
    print("{} has been successfully saved.".format(record))

    df_verified.to_csv(verified, index=False)
    print("{} has been successfully saved.".format(verified))

    df_notfound.to_csv(notfound, index=False)
    print("{} has been successfully saved.".format(notfound))


def parse_content(soup):
    '''
    parse from soup content and return found result in dictionary
    :param soup:
    :return:
    '''
    result = {}
    tickernodes = soup.find_all('div', {'class': 'fjfe-autocomplete-ticker'})
    # tickernodes = soup.find_all('div', {'role':'listbox'})
    if not tickernodes:
        return {}

    for tickernode in tickernodes:
        options = tickernode.find_all('div', {'class': 'ac-row', 'role': 'option'})
        if len(options) == 0:
            return {}
        else:
            option_index = 0
            for option in options:
                name = None
                exchange = None
                symbol = None

                exchanges = option.find_all('div', {'class': 'exchange'})
                if exchanges and len(exchanges) > 0:
                    exchange = exchanges[0].getText().strip()

                names = option.find_all('div', {'class': 'name'})
                if names and len(names):
                    name = names[0].getText().strip()

                symbols = option.find_all('div', {'class': 'symbol'})
                if symbols and len(symbols) > 0:
                    symbol = symbols[0].getText().strip()

                if symbol and exchange and name:
                    option_index += 1
                    one_result = {'symbol':symbol,'market':exchange,'name':name}
                    result[option_index] = one_result
                    print("{}->{}".format(option_index, one_result))

            return result

def main():
    driver, source, record, verified, notfound = helpers.cleanjohn_commandline_parser()

    print(driver, source, record, verified, notfound)
    verify_all(driver, source, record, verified, notfound)


if __name__ == '__main__':
    main()
