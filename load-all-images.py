#!/usr/bin/env python3.10
# Args:
## $1 - token in format "user:password" for API authentication
## $2 - dst json file with XXX sequence for replacing with backup number
## $3..$x - source csv files for searching & loading raw data from API

import csv
from datetime import datetime
import json
import re
import requests
from requests.auth import HTTPBasicAuth
import sys
import time

# Usually 1000 requests per 1 hour, but saving some jitter values
# https://support.atlassian.com/bitbucket-cloud/docs/api-request-limits/
BITBUCKET_RATE_LIMIT = 970
BITBUCKET_RATE_LIMIT_INTERVAL_SECONDS = 3600 + 60

API_PREFIX = "https://bitbucket.org/"
API_SUFFIX = ".png"
PATTER_REPLACE_VALUE = "XXX"

def init():
    try:
        # see https://stackoverflow.com/a/15063941/6818663
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        maxLong = (1 << 31) - 1

        # Looks like Windows uses long instead of long long
        csv.field_size_limit(maxLong)

def parse_args():
    USER_PASS = sys.argv[1]
    userPassSplit = USER_PASS.split(':')

    global AUTH
    AUTH = HTTPBasicAuth(userPassSplit[0], userPassSplit[1])

    global DST_FILE
    DST_FILE = sys.argv[2]

    global SRC_FILES
    SRC_FILES = []
    for p in sys.argv[3:]:
        SRC_FILES.append(p)

def single_query(url):
    res = requests.get(url, auth=AUTH)
    res.raise_for_status()

    return res.text

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

    res = [d for d in urls if d.startswith(API_PREFIX) and d.endswith(API_SUFFIX)]

    # need only unique urls
    res = set(res)
    return res

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
        except Exception as e:
            print(f"Exception was caught for url '{d}'")
            print(e)
            print()

        if i >= BITBUCKET_RATE_LIMIT:
            i = 0
            step += 1

            # temp saving
            jsonTxt = json.dumps(res)
            resFileName = DST_FILE.replace(PATTER_REPLACE_VALUE, str(step))
            with open(resFileName, "w", encoding="utf8") as f:
                f.write(jsonTxt)

            print("File", resFileName, "was written")

            newTs = ts + BITBUCKET_RATE_LIMIT_INTERVAL_SECONDS
            tsDiff = newTs - time.time()

            print(datetime.now(), "Will get up in", tsDiff, "seconds")
            time.sleep(tsDiff)

    # saving full results
    jsonTxt = json.dumps(res)
    resFileName = DST_FILE.replace(PATTER_REPLACE_VALUE, 'final')
    with open(resFileName, "w", encoding="utf8") as f:
        f.write(jsonTxt)

    print("Final file", resFileName, "was written")

def main():
    init()
    parse_args()

    data = load_csv_data()
    urls = select_only_urls(data)
    load_data_from_urls_with_backup(urls)

if __name__ == '__main__':
    main()
