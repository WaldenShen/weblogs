#!/usr/bin/python
# coding=UTF-8

import os
import re
import pickle
import gzip
import json
import datetime
import pprint

SEP = "\t"
OTHER = "其他"
NEXT = ">"
ENCODE_UTF8 = "UTF-8"

COOKIE_HISTORY = "cookie_history"

FUNC = lambda x, y: y + "_" + x if (x and x.lower() != "none") else y + "_" + OTHER
FUNC_NONE = lambda x: float(x) if (x and x.lower() != "none") else 0

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
FILEPATH_COOKIE_ID = os.path.join(BASEPATH, "data", "setting", "cookie_history.pkl")

def load_category(filepath):
    results = {}

    '''
    平台    產品1   產品2   Function    Intention   landing後的URL
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_4.asp
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_4_01.asp
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/Cathaybk/vip/myvip_4_02_01.asp
    官網    基金    基金    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_4_02_02.asp
    官網    個人信託    個人信託    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_4_02_03.asp
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/Cathaybk/vip/myvip_4_03_01.asp
    官網    財管    財管    理財    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_index.asp
    官網    財管    財管    產品    理財投資    https://www.cathayholdings.com/bank/cathaybk/vip/myvip_news1030820.asp
    ...
    ...
    ...
    '''
    with open(filepath, "r", encoding=ENCODE_UTF8) as in_file:
        is_header = True

        for line in in_file:
            if is_header:
                is_header = False
            else:
                info = re.split(",", line.strip().lower())

                website, product_1, product_2, function, intention, url = info
                results.setdefault(url, {"logic": product_1 + "_" + product_2, "function": function, "intention": intention})

    return results

def is_app_log(log):
    return log.startswith("app://")

def norm_url(url):
    start_idx = url.find("?")
    return url[:start_idx if start_idx > -1 else len(url)]

def get_date_type(filename):
    date = os.path.basename(filename).split("_")[1].split(".")[0]

    date_type = None
    if len(date) == 12:
        date = datetime.datetime.strptime(date, "%Y-%m-%d%H").strftime("%Y-%m-%d %H:00:00")
        date_type = "hour"
    elif len(date) == 10:
        date_type = "day"
    elif len(date) == 4:
        date_type = "year"
    elif date.upper().find("W") > -1:
        date_type = "week"
    else:
        date_type = "month"

    return date, date_type

def print_json(filepath):
    with gzip.open(filepath, "rb") as out_file:
        for line in out_file:
            j = json.loads(line.strip())
            pprint.pprint(j)

def load_cookie_history(filepath=FILEPATH_COOKIE_ID):
    results = None
    with open(filepath, "rb") as in_file:
        results = pickle.load(in_file)

    return results

def save_cookie_history(results, filepath=FILEPATH_COOKIE_ID):
    with open(filepath, "wb") as out_file:
        pickle.dump(results, out_file)

def create_cookie_history(filepath, pool={}):
    global ENCODE_UTF8, SEP

    with gzip.open(filepath, "rb") as in_file:
        is_header = True

        for line in in_file:
            if is_header:
                is_header = False
            else:
                o = json.loads(line.decode(ENCODE_UTF8))
                cookie_id, creation_datetime = o["cookie_id"], o["creation_datetime"]

                if creation_datetime.find(".") > -1:
                    creation_datetime = datetime.datetime.strptime(o["creation_datetime"], "%Y-%m-%d %H:%M:%S.%f")
                else:
                    creation_datetime = datetime.datetime.strptime(o["creation_datetime"], "%Y-%m-%d %H:%M:%S")

                pool.setdefault(cookie_id, []).append(creation_datetime)

    return pool

if __name__ == "__main__":
    import glob

    df = {}
    filepath_raw_cookie = os.path.join(BASEPATH, "data", "raw", "cookie_[0-9]*.tsv.gz")
    for filepath in glob.iglob(filepath_raw_cookie):
        if len(os.path.basename(filepath)) > 22:
            df = create_cookie_history(filepath, df)
            print("current filepath is {}".format(filepath))

    save_cookie_history(df)

    '''
    results = load_cookie_id()
    for cookie_id, creation_datetime in results.items():
        print((cookie_id, creation_datetime))
    '''
