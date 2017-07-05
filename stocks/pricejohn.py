#!/usr/bin/env python

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time

import os.path
import sys
import pandas as pd
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from selenium.common.exceptions import TimeoutException
from stocks import helpers
import requests
import logging
from http.client import RemoteDisconnected
from urllib.error import URLError

def download_googlepage_ticker(commandline_options, symbol, google_market,market_current, driver):
    ''' download price history from google page by page.'''
    # extract_stock_price(commandline_options, symbol, market, export_file, driver, search_elem, logger):
    from stocks import pricegoogle

    if (not google_market) or str(google_market)=='':
        market_path = "Unknown"
    else:
        market_path = str(google_market)

    download_path = os.path.join(helpers.get_exportdata_path(), 'pricegooglepage', market_path)
    if not os.path.exists(download_path):
        os.makedirs(download_path)
    download_file = os.path.join(download_path, "{}.{}.csv".format(symbol, google_market))

    if commandline_options['bypass'] and os.path.exists(download_file) and os.path.isfile(download_file):
        return False, True, download_file, "File exists, add it to record"

    is_fatal, driver, search_elem, note = pricegoogle.extract_stock_price(
        commandline_options,
        symbol,
        google_market,
        download_file,
        driver,
        None,
        None
    )

    return is_fatal, note=='Success', download_file, note

def download_google_ticker(commandline_options, symbol, google_market,market_current, driver):
    if (not google_market) or str(google_market)=='':
        market_path = "Unknown"
    else:
        market_path = str(google_market)

    download_path = os.path.join(helpers.get_exportdata_path(), 'pricegoogle', market_path)
    if not os.path.exists(download_path):
        os.makedirs(download_path)
    download_file = os.path.join(download_path, "{}.{}.csv".format(symbol, google_market))

    if commandline_options['bypass'] and os.path.exists(download_file) and os.path.isfile(download_file):
        return False, True,download_file,"File exists, add it to record"


    url = 'https://www.google.com/finance/historical?q={}:{}'.format(google_market, symbol)
    try:
        driver.set_page_load_timeout(15)
        print('url:\t{}'.format(url))
        driver.get(url)
    except TimeoutException:
        # should consider to return immediately?
        try:
            driver.execute_script("window.stop();")
        except TimeoutException:
            return True, False, "N/A", "Time out to stop loading. OS may be too busy."
    except ConnectionRefusedError:
        return True, False, "N/A", "Connection Refused Error"

    IS_PHANTOMJS = commandline_options['is_phantom']
    if IS_PHANTOMJS:
        print('before slepping, content length:{}, sleeping...'.format(len(driver.page_source)))
        time.sleep(commandline_options['dropwait'])
        print('after sleep:{}. Hope node has been loaded'.format(len(driver.page_source)))
    else:
        time.sleep(commandline_options['dropwait'])

    try:
        search_elem = driver.find_element_by_id('gbqfq')    #google search box
    except NoSuchElementException:
        return False, False, "N/A",   "Can't find search box!"

    if not search_elem:
        return False, False, "N/A", "Can't find search box correctly."

    try:
        start_date_elem = driver.find_element_by_class_name('id-fromdate')
    except NoSuchElementException:
        return False, False, "N/A", "Can't find start date element."

    try:
        update_button_elem = driver.find_element_by_id('hfs')
    except NoSuchElementException:
        return False, False, "N/A",  "Can't find update button"

    if not (start_date_elem and update_button_elem):
        return False, False, "N/A",  "Can't find start date or update button."
    try:
        driver.execute_script("arguments[0].value = arguments[1]", start_date_elem, "Jan 1, 2000")
    except TimeoutException:
        return False, False, "N/A", "Timeout in arguments[0].value = arguments[1] start_date_elem."
    try:
        update_button_elem.click()
    except TimeoutException:
        return False, False, "N/A", "Timeout in update_button_elem.click()."

    try:
        time.sleep(commandline_options['downloadwait'])
        download_elem = driver.find_element_by_xpath("//div/img[@class='SP_download']/../a")
    except NoSuchElementException:
        return False, False, "N/A",  "Can't find download element"

    if not download_elem:
        return False, False, "N/A", "Can't find download link"

    session = requests.Session()
    cookies = driver.get_cookies()
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'])

    try:
        response = session.get(download_elem.get_attribute('href'))

        if len(response.content) < 20:
            return False, True, None, "Success but wrong content maybe"

        with open(download_file, "wb") as exportfile:
            exportfile.write(response.content)

        print("download file to:{}".format(download_file))
        return False, True, download_file, "Success"
    except:
        # helpers.PrintException()
        time.sleep(5 * 60)  # sleep 5 minutes to relex CPU
        return False, False, None, "Failed in downloading or saving or CPU busy, it may be link expired."

def download_yahoo_ticker(commandline_options, symbol, google_market,market_current, driver):
    '''

    :param symbol:
    :param yahoo_market:
    :param driver:
    :return: errorfound, contentfound,download_file,errormsg
    '''
    if pd.isnull(market_current):
        # this market doesn't exist in yahoo
        return False, False, "N/A", "This market doesn't exist in Yahoo!"

    if (not google_market) or str(google_market)=='':
        market_path = "Unknown"
    else:
        market_path = str(google_market)

    # https://finance.yahoo.com/quote/ITX.L/history
    # be careful: yahoo market has ".", for example: "."
    yahoo_market = str(market_current)
    if yahoo_market in ['NYSE', 'NASDAQ']:
        yahoo_market = ""

    download_path = os.path.join(helpers.get_exportdata_path(), 'price', market_path)
    if not os.path.exists(download_path):
        os.makedirs(download_path)
    download_file = os.path.join(download_path, "{}{}.csv".format(symbol, yahoo_market))

    if commandline_options['bypass'] and os.path.exists(download_file) and os.path.isfile(download_file):
        return False, True,download_file,"File exists, add it to record"

    url = "https://finance.yahoo.com/quote/{}{}/history".format(symbol,yahoo_market)
    print("url={}".format(url))

    IS_PHANTOMJS = commandline_options['is_phantom']
    try:
        driver.set_page_load_timeout(commandline_options['dropwait'])
        driver.get(url)
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except TimeoutException:
            return True, False, "N/A", "Time out to stop loading. OS may be too busy."
    except ConnectionRefusedError:
        return True, False, "N/A", "Connection Refused Error"

    date_range_dropdown=None
    if IS_PHANTOMJS:
        print('before slepping, content length:{}, sleeping...'.format(len(driver.page_source)))
        time.sleep(5)
        print('after sleep:{}. Hope node has been loaded'.format(len(driver.page_source)))
    try:
        date_range_dropdown = driver.find_element_by_xpath("//input[@data-test='date-picker-full-range']")
    except:
        print("date_range_dropdown->Unexpected error:", sys.exc_info()[0])
        date_range_dropdown = None

    if not date_range_dropdown:
        return False, False, "N/A","Can't find date_range_dropdown"

    span_max = None
    date_range_dropdown.click()
    if IS_PHANTOMJS:
        time.sleep(1)
    try:
        span_max = driver.find_element_by_xpath("//span[@data-value='MAX']")
    except:
        print("span_max->Unexpected error:", sys.exc_info()[0])
        span_max = None

    if not span_max:
        return False, False, "N/A", "Can't find span_max"

    button_done = None
    span_max.click()
    if IS_PHANTOMJS:
        time.sleep(1)
    try:
        button_done = driver.find_element_by_xpath("//span[text()='Done']/..")
    except:
        print("button_done->Unexpected error:", sys.exc_info()[0])
        button_done = None

    if not button_done:
        return False, False, "N/A", "Can't find button_done"

    button_apply = None
    button_done.click()
    if IS_PHANTOMJS:
        time.sleep(1)
    try:
        button_apply = driver.find_element_by_xpath("//button/span[text()='Apply']/..")
    except:
        print("button_apply->Unexpected error:", sys.exc_info()[0])
        button_apply = None

    if not button_apply:
        return False, False, "N/A", "Can't find button_apply"

    download_href = None
    button_apply.click()
    time.sleep(commandline_options['downloadwait'])
    try:
        download_href = driver.find_element_by_xpath("//a/span[text()='Download Data']/..")
    except:
        print("download_href->Unexpected error:", sys.exc_info()[0])
        download_href = None

    if not download_href:
        return False, False, "N/A", "Can't find download_href"

    session = requests.Session()
    cookies = driver.get_cookies()
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'])

    try:
        response = session.get(download_href.get_attribute('href'))

        if len(response.content) < 100:
            return False, True, None, "Success but wrong content maybe"

        with open(download_file, "wb") as exportfile:
            exportfile.write(response.content)

        print("download file to:{}".format(download_file))
        return False, True, download_file, "Success"
    except:
        #helpers.PrintException()
        time.sleep(5*60)    #sleep 5 minutes to relex CPU
        return False, False, None, "Failed in downloading or saving or CPU busy, it may be link expired."


def convert_market_google_2_yahoo(df_market, market):
    df_local_filtered = df_market[df_market['Google Finance Symbol']==market]
    if len(df_local_filtered)>0:
        return df_local_filtered.iloc[0]['Yahoo! Finance Symbol']
    else:
        return None

def download_ticker(commandline_options,symbol, google_market,market_current,driver):
    '''

    :param symbol:
    :param market:
    :param yahoo_market:
    :param driver:
    :return: errorfound, contentfound,download_file,errormsg
    '''
    is_yahoo_market = commandline_options['is_yahoo']
    if is_yahoo_market:
        return download_yahoo_ticker(commandline_options, symbol, google_market,market_current, driver)
    elif commandline_options['googlepage']:
        return download_googlepage_ticker(commandline_options, symbol, google_market,market_current, driver)
    else:
        return download_google_ticker(commandline_options, symbol, google_market, market_current, driver)

def process(commandline_options):
    logger = commandline_options['logger']

    sourcefile = os.path.join(helpers.get_basedata_path(), commandline_options['source'])

    marketlistfile = os.path.join(helpers.get_basedata_path(),'markets_list.csv')

    symbol_col = commandline_options['symbol']
    market_col = commandline_options['market']
    df_source = pd.read_csv(sourcefile)
    df_source_total = len(df_source.index)

    recordfile = None
    df_source_start_index = 0
    df_source_end_index = df_source_total
    if commandline_options['piece'] and df_source_total > commandline_options['total']+10:
        # if too small, we will not split
        df_source_start_index = int(df_source_total * (commandline_options['piece'] -1) / commandline_options['total'])
        df_source_end_index  = int(df_source_total * (commandline_options['piece']) / commandline_options['total'])
        print("This downloading thread will focus on index between {} and {}.".format(df_source_start_index,
                                                                                      df_source_end_index))
        # iloc slice will not include end index.
        df_source = df_source.iloc[df_source_start_index:df_source_end_index]

        recordfile = os.path.join(helpers.get_basedata_path(),
                                  "price_download_{}of{}_".format(commandline_options['piece'],
                                                                  commandline_options['total'])
                                  + commandline_options['source'])
    else:
        recordfile = os.path.join(helpers.get_basedata_path(), "price_download_" + commandline_options['source'])

    dtypedic = {}
    dtypedic[symbol_col] = object
    dtypedic[market_col] = object

    df_record = helpers.get_or_create_dataframe(recordfile,
                                                [symbol_col, market_col,'yahoo_market','processed', 'exportfile','note'],
                                                dtypedic)
    df_market = pd.read_csv(marketlistfile)


    record_index = len(df_record.index)
    print("existing record:{}".format(record_index))

    df_filtered = df_source[~((df_source[symbol_col].isin(df_record[symbol_col]))
                              & (df_source[market_col].isin(df_record[market_col])))]
    export_path = helpers.get_exportdata_path()

    '''initialize web driver'''
    driver = None
    commandline_options['is_phantom'] = False
    if 'gecko' in commandline_options['driver']:
        firefoxProfile = webdriver.FirefoxProfile()
        firefoxProfile.set_preference('permissions.default.stylesheet', 2)
        firefoxProfile.set_preference('permissions.default.image', 2)
        firefoxProfile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
        firefoxProfile.set_preference("http.response.timeout", 10)
        firefoxProfile.set_preference("dom.max_script_run_time", 10)
        # add profile for better timeout control.
        driver = webdriver.Firefox(executable_path=commandline_options['driver'],
                                   firefox_profile=firefoxProfile)
    elif 'chrome' in commandline_options['driver']:
        # "C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe"
        if not os.path.exists(commandline_options['driver']):
            raise IOError("Can't find chrome webdriver:{}".format(commandline_options['driver']))

        os.environ['webdriver.chrome.driver'] = commandline_options['driver']
        driver = webdriver.Chrome(commandline_options['driver'])
    else:
        commandline_options['is_phantom'] = True    #has to provide more time sleep
        '''
        dcap = dict(DesiredCapabilities.PHANTOMJS)
        dcap["phantomjs.page.settings.userAgent"] = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/53 "
            "(KHTML, like Gecko) Chrome/15.0.87"
        )
        '''
        driver = webdriver.PhantomJS(executable_path=commandline_options['driver'])
        ''', desired_capabilities=dcap)'''

    is_yahoo_market=False
    commandline_options['is_yahoo'] = False
    commandline_options['googlepage'] = False
    if commandline_options['target'] == 'yahoo':
        is_yahoo_market = True
        commandline_options['is_yahoo'] = True
    elif commandline_options['target'] == 'page':
        commandline_options['googlepage'] = True
    else:
        commandline_options['googlepage'] = False

    try:
        for index, row in df_filtered.iterrows():
            symbol_current = row[symbol_col]
            market_current = None
            if is_yahoo_market:
                market_current = convert_market_google_2_yahoo(df_market, row[market_col])
                if not market_current:
                    #can't find proper yahoo market
                    print("({}->{}<-{})->{}".format(df_source_start_index, index, df_source_end_index,
                                                    [symbol_current, row[market_col], "N/A", "False", "N/A",
                                                     "Can't find proper yahoo market"]))

                    df_record.loc[record_index] = [symbol_current, row[market_col],"N/A", "False", "N/A",
                                                   "Can't find proper yahoo market"]
                    record_index += 1
                    continue
            else:
                market_current = row[market_col]

            try:
                download_file = None
                errorfound, contentfound,download_file,errormsg=download_ticker(commandline_options,
                                                                            symbol_current,
                                                                            row[market_col],
                                                                            market_current,
                                                                            driver)
            except TimeoutException:
                errorfound = False
                contentfound = False
                errormsg = "Timeout exception happened when downloading"
            except ConnectionRefusedError:
                errorfound = True
                contentfound = False
                errormsg = "ConnectionRefusedError happened when downloading"
            except RemoteDisconnected:
                errorfound = True
                contentfound = False
                errormsg = "RemoteDisconnected happened when downloading"
            except URLError:
                errorfound = True
                contentfound = False
                errormsg = "URLError happened when downloading"

            logger.info("({}->{}<-{})->{}".format(df_source_start_index,index,df_source_end_index,
                                            [symbol_current,row[market_col],market_current,
                                               contentfound,download_file,errormsg]))
            if errorfound:
                print("Fatal Error found, quit!")
                break

            if is_yahoo_market:
                #print(df_record.columns.values)
                df_record.loc[record_index] = [symbol_current,row[market_col],market_current,contentfound,download_file,errormsg]
            else:
                df_record.loc[record_index] = [symbol_current, market_current,"N/A", contentfound,download_file, errormsg]
            record_index += 1

    except KeyboardInterrupt:
        print("Keyboard Interrupt")

    df_record.to_csv(recordfile,index=False)
    print("total {} file processed!".format(len(df_record.index)))


def main():
    commandline_options = helpers.pricejohn_commandline_parser()
    sourcefile = os.path.join(
        helpers.get_basedata_path(),
        commandline_options['source']
    )

    if not (os.path.exists(sourcefile) and os.path.isfile(sourcefile)):
        raise IOError("Can't find input csv file:{}".format(sourcefile))

    logger = logging.getLogger(__name__)
    logger.setLevel(helpers.get_logger_level(commandline_options.get('loglevel','INFO')))

    file_handler = logging.FileHandler(
        os.path.join(helpers.get_exportdata_path(),
                     commandline_options['source'].split(".")[0] + "_download.log")
    )
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    screen_handler = logging.StreamHandler()
    screen_handler.setLevel(helpers.get_logger_level(commandline_options.get('loglevel','INFO')))
    logger.addHandler(screen_handler)

    commandline_options['logger'] = logger

    try:
        process(commandline_options)
    except Exception as e:
        logger.info("Unknown exception", exc_info=True)


if __name__ == '__main__':
    main()
