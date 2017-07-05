#!/usr/bin/env python

from selenium import webdriver
from selenium.webdriver.common.by import By

from selenium.common.exceptions import NoSuchElementException
import time

from http.client import RemoteDisconnected
import logging
import os.path
from selenium.webdriver.common.keys import Keys
import sys
from datetime import datetime
import pandas as pd
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from selenium.common.exceptions import TimeoutException
from stocks import helpers
import requests
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from urllib.error import URLError

'''
purpose of this module:
1) better exception control.
2) better logger
3) less memory consumption
4) extract stock history info from google (since yahoo only has limited offering.)
'''


def extract_stock_price(commandline_options, symbol, market, export_file, driver, search_elem, logger=None):
    '''

    :param commandline_options:
    :param symbol:
    :param market:
    :param export_file:
    :param driver:
    :param search_elem:
    :param logger:
    :return: 1. Fatal error or not.
    '''
    if not logger:
        logger = commandline_options['logger']

    full_path_mode = commandline_options.get("entry_mode","fullpath") == "fullpath"
    #after a while, Google will refuse to respond url to history price directly.
    #in stead, we will start from finance.google.com and then enter tikcer
    # and then click search, and then click history price link.
    if not full_path_mode:
        # this is a new page
        url = 'https://www.google.com/finance/historical?q={}:{}'.format(market, symbol)
        try:
            driver.set_page_load_timeout(15)
            print('url:\t{}'.format(url))
            driver.get(url)
        except TimeoutException:
            # should consider to return immediately?
            return True, driver, search_elem, "Timeout when open {}".format(url)
        except ConnectionRefusedError:
            return True, driver, search_elem, "ConnectionRefusedError {}".format(url)
        except URLError:
            return True, driver, search_elem, "URLError[Error 111] before {}".format(url)
    else:
        url = 'https://finance.google.com'
        try:
            driver.set_page_load_timeout(10)
            print('url:\t{} for {}:{}'.format(url, market,symbol))
            driver.get(url)
        except TimeoutException:
            # should consider to return immediately?
            return True, driver, search_elem, "Timeout when open {}".format(url)
        except ConnectionRefusedError:
            return True, driver, search_elem, "ConnectionRefusedError {}".format(url)
        except URLError:
            return True, driver, search_elem, "URLError[Error 111] before {}".format(url)

        try:
            time.sleep(8)
            search_elem = driver.find_element_by_id('gbqfq')  # google search box
        except NoSuchElementException:
            return True, driver, search_elem, "Can't find search box!"

        if not search_elem:
            return True, driver, search_elem, "Can't find search box correctly."

        #search_elem.clear()
        try:
            search_elem.send_keys("{}:{}".format(market, symbol))
            search_elem.send_keys(Keys.RETURN)

        except TimeoutException:
            return True, driver, search_elem, "Timeout when send_keys{}:{}".format(market, symbol)

        try:
            time.sleep(5)
            history_price_link = driver.find_element_by_xpath("//li/a[text()='Historical prices']")
        except NoSuchElementException:
            return False, driver, search_elem, "Can't find history price link for {}:{}".format(market, symbol)

        try:
            history_price_link.click()
        except TimeoutException:
            return False, driver, search_elem, "Timeout when click history price link for {}:{}".format(market, symbol)

    start_date = commandline_options.get("startdate","20000101")
    #convert to Jan 01, 2000, then to Jan 1, 2000
    start_date = datetime.strftime(datetime.strptime(start_date,"%Y%m%d"), "%b %d, %Y").replace(' 0',' ')

    try:
        time.sleep(10)
        start_date_elem = driver.find_element_by_class_name('id-fromdate')
    except NoSuchElementException:
        return False,  driver, search_elem, "Can't find start date element."

    try:
        update_button_elem = driver.find_element_by_id('hfs')
    except NoSuchElementException:
        return False,  driver, search_elem, "Can't find update button"

    if not (start_date_elem and update_button_elem):
        return False,  driver, search_elem, "Can't find start date or update button."

    driver.execute_script("arguments[0].value = arguments[1]", start_date_elem, start_date)
    update_button_elem.click()

    try:
        time.sleep(5)
        tbody_elem = driver.find_element_by_xpath("//table[@class='gf-table historical_price']/tbody")
    except NoSuchElementException:
        return False, driver, search_elem, "Can't find table body"

    try:
        trows_elem = tbody_elem.find_elements(By.TAG_NAME,"tr")
    except NoSuchElementException:
        return False, driver, search_elem, "Can't find rows in tbody."

    first_page = True
    has_next_page = True

    row_header = []
    tempfile =export_file + "_temp"

    with open(tempfile, "w") as csvfile:
        page_index = 1
        while has_next_page:
            logger.info("{}:{} at Page:{}".format(market, symbol, page_index))
            page_index += 1
            row_index = 0
            last_row = len(trows_elem) - 1

            for row in trows_elem:
                row_fields = []

                if row_index == 0:
                    if first_page:
                        first_page = False
                        for th_elem in row.find_elements(By.TAG_NAME,"th"):
                            row_header.append(th_elem.text)
                        #print("Header:{}".format(row_header))

                        csvfile.write(",".join(row_header) +"\n")
                    row_index += 1
                elif row_index == last_row:
                    try:
                        next_page_elem = row.find_element(By.CLASS_NAME, "SP_arrow_next")
                    except NoSuchElementException:
                        next_page_elem = None

                    if not next_page_elem:
                        logger.debug("No more page")
                        has_next_page = False
                    else:
                        logger.debug("Try to find next page.")
                        next_page_elem.click()
                        try:
                            time.sleep(5)
                            tbody_elem = driver.find_element_by_xpath(
                                "//table[@class='gf-table historical_price']/tbody")
                            try:
                                trows_elem = tbody_elem.find_elements(By.TAG_NAME, "tr")
                            except NoSuchElementException:
                                has_next_page = False
                        except NoSuchElementException:
                            has_next_page = False

                        if has_next_page and (not trows_elem):
                            has_next_page = False
                            logger.debug("Can't find new page after click.")

                    break
                else:
                    td_index = 0
                    for td_elem in row.find_elements(By.TAG_NAME,"td"):
                        try:
                            td_text = td_elem.text
                        except RemoteDisconnected:
                            logger.debug("Remote disconnected when parse TD.")
                            return False,  driver, search_elem, "Remote Disconnected without response."
                        if td_index == 0:
                            # column of date in format Aug 24, 2016
                            # it will be converted to format YYYYMMDD 20160824
                            datetime_object = datetime.strptime(td_text, "%b %d, %Y")
                            row_fields.append(datetime_object.strftime("%Y%m%d"))
                            td_index += 1
                            continue

                        if 5>td_index >0:
                            #fields: Open, High, Low, Close
                            #possible data: "-", 0.42 etc.
                            if td_text == "-":
                                row_fields.append("")
                            else:
                                floating_object = float(td_text.replace(",","")) #assume US format
                                row_fields.append("{:0.2f}".format(floating_object))

                            td_index += 1
                            continue

                        if td_index == 5:
                            #Volume field, "20,000" etc.
                            if td_text == "-":
                                row_fields.append("")
                            else:
                                floating_object = float(td_text.replace(",", ""))  # assume US format
                                row_fields.append("{:0.0f}".format(floating_object))

                            td_index += 1
                            continue

                        #row_fields.append(td_elem.text)

                    logger.debug("{}:{}".format(row_index, row_fields))
                    csvfile.write(",".join(row_fields) +"\n")
                    row_index += 1

    #after success, change tempfile to normail file.
    os.rename(tempfile,export_file)

    return False,  driver, search_elem, "Success"



def filter_inputfile(commandline_options, input_file, record_file,cols,logger):
    pass

def main():
    commandline_options = {}
    market = "NASDAQ"
    symbol = "GOOG"
    export_file = os.path.join(helpers.get_exportdata_path(),"google_price_test.csv")

    driver = helpers.get_driver()
    search_elem = None

    is_fatal, driver, search_elem, note = extract_stock_price(commandline_options,
                                                              symbol, market, export_file, driver, search_elem,
                                                              None)

    print(is_fatal, driver, search_elem, note)



if __name__ == '__main__':
    main()
