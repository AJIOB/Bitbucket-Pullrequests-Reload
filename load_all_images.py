#!/usr/bin/env python3.10
# Args:
## $1 - token in format "user:password" for API authentication
## $2 - dst folder with XXX sequence for replacing with backup number
## $3..$x - source csv files for searching & loading raw data from API

import csv
from datetime import datetime
import os
import re
import requests
from requests.auth import HTTPBasicAuth
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
import sys
import time
from urllib.parse import unquote

# Usually 1000 requests per 1 hour, but saving some jitter values
# https://support.atlassian.com/bitbucket-cloud/docs/api-request-limits/
BITBUCKET_RATE_LIMIT = 970
BITBUCKET_RATE_LIMIT_INTERVAL_SECONDS = 3600 + 60

API_PREFIX = "https://bitbucket.org/"
API_CONTAINS = "/images/"
PATTER_REPLACE_VALUE = "XXX"

IS_USE_SELENIUM = True
TIME_TO_AUTH_SECONDS = 45

def init():
    try:
        # see https://stackoverflow.com/a/15063941/6818663
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        maxLong = (1 << 31) - 1

        # Looks like Windows uses long instead of long long
        csv.field_size_limit(maxLong)

    # Enable single driver instance if need
    if IS_USE_SELENIUM:
        global SELENIUM_DRIVER
        SELENIUM_DRIVER = webdriver.Firefox()

def deinit():
    if IS_USE_SELENIUM:
        global SELENIUM_DRIVER
        SELENIUM_DRIVER.quit()

def parse_args(argv):
    USER_PASS = argv[1]
    userPassSplit = USER_PASS.split(':')

    global AUTH
    AUTH = HTTPBasicAuth(userPassSplit[0], userPassSplit[1])

    global DST_FILE
    DST_FILE = argv[2]

    global SRC_FILES
    SRC_FILES = []
    for p in argv[3:]:
        SRC_FILES.append(p)

def single_query_selenium_get_true_url(url):
    global SELENIUM_DRIVER
    SELENIUM_DRIVER.get(url)

    rawImagePath = '//img'
    try:
        # get the image source
        img = SELENIUM_DRIVER.find_element(By.XPATH, rawImagePath)
    except NoSuchElementException:
        print(f"Cannot find image. Try to log in that page. You have {TIME_TO_AUTH_SECONDS} seconds")

        time.sleep(TIME_TO_AUTH_SECONDS)

        # get the image source again
        img = SELENIUM_DRIVER.find_element(By.XPATH, rawImagePath)

    src = img.get_attribute('src')

    return src

def single_query(url):
    if IS_USE_SELENIUM:
        url = single_query_selenium_get_true_url(url)
        res = requests.get(url)
    else:
        res = requests.get(url, auth=AUTH)

    res.raise_for_status()

    return res.content

def load_csv_data():
    res = []

    for f in SRC_FILES:
        with open(f, "r", encoding="utf8") as src:
            inReader = csv.reader(src)

            for row in inReader:
                for r in row:
                    res.append(r)

    return res

def select_only_urls(data):
    urls = []

    for d in data:
        # URL match regex from https://uibakery.io/regex-library/url-regex-python
        matches = re.findall(r'https?:\/\/(?:www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b(?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)', d)

        for m in matches:
            urls.append(m)

    res = [d for d in urls if d.startswith(API_PREFIX) and API_CONTAINS in d]
    for i, r in enumerate(res):
        if r.endswith(')'):
            # That is regex bug
            res[i] = r[:-1]

    # need only unique urls
    res = list(set(res))
    return res

def dump_results(path, obj):
    try:
        # recursive creation
        os.makedirs(path, exist_ok=True)

        for o in obj:
            blob = obj[o]

            fileName = unquote(o).replace(':', '_').replace('/', '_')

            with open(path + os.path.sep + fileName, "wb") as f:
                f.write(blob)
    except Exception as e:
        print(f"Exception was caught for results dumping to path '{path}'")
        print(e)
        print()

def load_data_from_urls_with_backup(urls):
    step = 0
    i = 0

    # For saving results
    res = {}

    for d in urls:
        if i == 0:
            ts = time.time()

        i += 1

        try:
            res[d] = single_query(d)
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Exception was caught for url '{d}'")
            print(f"HTTP code {e.response.status_code}")
            print(e.response.text)
            print()
            if e.response.status_code == 401:
                print("Cannot authorize on server. Bad credentials or no permissions or OAuth2 is required (not supported). Exiting...")
                if not IS_USE_SELENIUM:
                    print("Try to enable Selenium")
                print()
                exit(1)
        except NoSuchElementException as e:
            print(f"Cannot find image element in Selenium for url {d}. Skipping it.")
            print()
        except Exception as e:
            print(f"Exception was caught for url '{d}'")
            print(e)
            print()

        if i >= BITBUCKET_RATE_LIMIT:
            i = 0
            step += 1

            # temp saving
            resFileName = DST_FILE.replace(PATTER_REPLACE_VALUE, str(step))
            dump_results(resFileName, res)

            print("Dump", resFileName, "was written")

            newTs = ts + BITBUCKET_RATE_LIMIT_INTERVAL_SECONDS
            tsDiff = newTs - time.time()

            print(datetime.now(), "Will get up in", tsDiff, "seconds")
            time.sleep(tsDiff)

    # saving full results
    resFileName = DST_FILE.replace(PATTER_REPLACE_VALUE, 'final')
    dump_results(resFileName, res)

    print("Final dump", resFileName, "was written")

def main(argv):
    try:
        init()
        parse_args(argv)

        data = load_csv_data()
        urls = select_only_urls(data)
        load_data_from_urls_with_backup(urls)
    finally:
        deinit()

if __name__ == '__main__':
    main(sys.argv)
