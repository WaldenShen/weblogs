#!/usr/bin/python
# coding=UTF-8

import os
import re
import glob
import redis
import pickle
import gzip
import json
import datetime
import pprint

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

SEP = "\t"
NEXT = ">"
ENCODE_UTF8 = "UTF-8"

COUNT = "count"
FUNCTION = "function"
LOGIC1 = "logic1"
LOGIC2 = "logic2"
INTENTION = "intention"

INTERVAL = "interval"

UNKNOWN = u"未分類"
HOMEPAGE = u"首頁"
OTHER = "其他"

ALL_CATEGORIES = [LOGIC1, LOGIC2, FUNCTION, INTENTION, "logic", "logic1_function", "logic2_function", "logic1_intention", "logic2_intention"]

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")
FILEPATH_COOKIE_ID = os.path.join(BASEPATH, "data", "setting", "cookie_history.pkl")
FILEPATH_CATEGORY = os.path.join(BASEPATH, "data", "setting", "category.tsv")

'''
Error
KOKOWEB
MMB
MYBANK
my bank
myb2b
mybank
官網
平台
網站索引
'''
DOMAIN_MAP = {"com.cathaybk.koko.ios.app": "KOKO未分類",
              "www.cathayholdings.com": "官網未分類",
              "www.myb2b.com.tw": "企業網銀未分類",
              "com.cathaybk.mmb.ios.app": "行動銀行未分類",
              "com.cathaybk.mymobibank.android.app": "行動銀行未分類",
              "www.cathaybk.com.tw": "官網未分類",
              "www.globalmyb2b.com": "企業網銀未分類",
              "www.mybank.com.tw": "網路銀行未分類",
              "com.cathaybk.koko.android.app": "KOKO未分類",
              "www.kokobank.com": "KOKO未分類",
              "cathaybk.com.tw": "官網未分類"}

FUNC = lambda x, y: y + "_" + x if (x and (isinstance(x, str) or isinstance(x, unicode)) and x.lower() != "none") else y + "_" + OTHER
FUNC_NONE = lambda x: float(x) if (x and x.lower() != "none") else 0

CATEGORY_URL = None


def load_category(filepath=FILEPATH_CATEGORY):
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
    in_file = None

    try:
        in_file = open(filepath, "r", encoding=ENCODE_UTF8)
    except:
        in_file = open(filepath, "r")

    is_header = True
    for line in in_file:
        if is_header:
            is_header = False
        else:
            info = re.split(",", line.strip().lower())

            website, product_1, product_2, function, intention, url = info
            if product_1:
                results.setdefault(url, {"logic1": product_1.lower().replace(" ", ""),
                                         "logic2": product_2.lower().replace(" ", ""),
                                         "function": function.lower().replace(" ", ""),
                                         "intention": intention})

    in_file.close()

    return results

def is_app_log(log):
    return log.startswith("app://")

def is_uncategorized_key(s):
    global UNKNOWN, HOMEPAGE, OTHER

    is_uncategorized = False
    if s.find(UNKNOWN) > -1 or s.find(HOMEPAGE) > -1 or s.find(unicode(OTHER, ENCODE_UTF8)) > -1:
        is_uncategorized = True

    return is_uncategorized

def norm_url(url):
    start_idx = url.find("?")
    return url[:start_idx if start_idx > -1 else len(url)].replace("http://", "https://")

def norm_category(name):
    return name.replace(" ", "").replace(u"投資理財", u"理財投資")

def _categorized_url(url, otype="all"):
    global CATEGORY_URL, DOMAIN_MAP, FUNC, OTHER
    global LOGIC1, LOGIC2, FUNCTION, INTENTION

    if CATEGORY_URL is None:
        CATEGORY_URL = load_category()

    n_url = norm_url(url.lower())

    logic1, logic2, function, intention = None, None, None, None
    if n_url in CATEGORY_URL:
        logic1, logic2, function, intention = CATEGORY_URL[n_url][LOGIC1], CATEGORY_URL[n_url][LOGIC2], CATEGORY_URL[n_url][FUNCTION], CATEGORY_URL[n_url][INTENTION]
    else:
        domain = DOMAIN_MAP.get(urlparse(n_url).netloc, OTHER)
        logic1, logic2, function, intention = domain, domain, domain, domain

    ret = None
    if otype == "all":
        ret = FUNC(logic1, LOGIC1), FUNC(logic2, LOGIC2), FUNC(function, FUNCTION), FUNC(intention, INTENTION)
    elif otype == LOGIC1:
        ret = FUNC(logic1, LOGIC1)
    elif otype == LOGIC2:
        ret = FUNC(logic2, LOGIC2)
    elif otype == FUNCTION:
        ret = FUNC(function, FUNCTION)
    elif otype == INTENTION:
        ret = FUNC(intention, INTENTION)
    elif otype == "logic":
        ret = FUNC(logic1, LOGIC1) + "_" + FUNC(logic2, LOGIC2)
    elif otype == "logic1_function":
        ret = FUNC(logic2, LOGIC1) + "_" + FUNC(function, FUNCTION)
    elif otype == "logic2_function":
        ret = FUNC(logic2, LOGIC2) + "_" + FUNC(function, FUNCTION)
    elif otype == "logic1_intention":
        ret = FUNC(logic1, LOGIC1) + "_" + FUNC(intention, INTENTION)
    elif otype == "logic2_intention":
        ret = FUNC(logic2, LOGIC2) + "_" + FUNC(intention, INTENTION)
    else:
        raise NotImplementedError

    return ret

def _rich_url(logic1, logic2, function, intention):
    return logic1 + "_" + logic2 , logic1 + "_" + function, logic2 + "_" + function, logic1 + "_" + intention, logic2 + "_" + intention

def parse_raw_page(line):
    global ENCODE_UTF8, SEP, FUNC_NONE

    session_id, cookie_id, individual_id, _, url, creation_datetime, duration, active_duration, loading_duration, _ = line.decode(ENCODE_UTF8).strip().split(SEP)
    logic1, logic2, function, intention = _categorized_url(url)
    logic, logic1_function, logic2_function, logic1_intention, logic2_intention = _rich_url(logic1, logic2, function, intention)

    return session_id, cookie_id, individual_id, url, creation_datetime,\
           logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention,\
           FUNC_NONE(duration), FUNC_NONE(active_duration), FUNC_NONE(loading_duration)

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

def parse_datetime(creation_datetime):
    if creation_datetime.find(".") > -1:
        creation_datetime = datetime.datetime.strptime(creation_datetime, "%Y-%m-%d %H:%M:%S.%f")
    else:
        creation_datetime = datetime.datetime.strptime(creation_datetime, "%Y-%m-%d %H:%M:%S")

    return creation_datetime

def unknown_urls(file_raw_page=os.path.join(BASEPATH, "data", "temp", "page_2016-09-21*.tsv.gz")):
    urls = {}
    for filepath in sorted(glob.glob(filepath_raw_page)):
        if len(os.path.basename(filepath)) > 20:
            with gzip.open(filepath) as in_file:
                is_header = True
                for line in in_file:
                    if is_header:
                        is_header = False
                    else:
                        info = parse_raw_page(line)
                        url, logic1 = info[3], info[5]

                        if not is_app_log(url) and OTHER in logic1:
                            urls.setdefault(norm_url(url), 0)
                            urls[norm_url(url)] += 1

    with open("unknown_url.txt", "wb") as out_file:
        for url, count in urls.items():
            out_file.write(url.encode(ENCODE_UTF8))
            out_file.write(SEP)
            out_file.write(str(count))
            out_file.write("\n")

def norm_str(value):
    global ENCODE_UTF8

    return re.sub("({}_|{}_|{}_|{}_)".format(LOGIC1, LOGIC2, FUNCTION, INTENTION), "", value).strip("_")

if __name__ == "__main__":
    '''
    # Create the login_datetime database

    filepath_raw_cookie = os.path.join(BASEPATH, "data", "raw", "cookie_[0-9]*.tsv.gz")
    for filepath in sorted(glob.glob(filepath_raw_cookie)):
        if len(os.path.basename(filepath)) > 22:
            create_cookie_history(filepath)
            print("current filepath is {}".format(filepath))
    '''

    out_file = gzip.open("cookie_behavior.gz", "ab")
    for filepath in sorted(glob.glob(os.path.join(BASEPATH_RAW, "cookie_*.tsv.gz"))):
        print("Start to proceed {}".format(filepath))
        create_behavior(filepath, out_file)
    out_file.close()
