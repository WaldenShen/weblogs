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
OTHER = "其他"
NEXT = ">"
ENCODE_UTF8 = "UTF-8"

COOKIE_HISTORY = "cookie_history"

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
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
DOMAIN_MAP = {"com.cathaybk.koko.ios.app": "KOKO",
              "www.cathayholdings.com": "官網",
              "www.myb2b.com.tw": "企業網銀",
              "com.cathaybk.mmb.ios.app": "行動銀行",
              "com.cathaybk.mymobibank.android.app": "行動銀行",
              "www.cathaybk.com.tw": "官網",
              "www.globalmyb2b.com": "企業網銀",
              "www.mybank.com.tw": "網路營行",
              "com.cathaybk.koko.android.app": "KOKO",
              "www.kokobank.com": "KOKO",
              "cathaybk.com.tw": "官網"}

FUNC = lambda x, y: y + "_" + x if (x and (isinstance(x, str) or isinstance(x, unicode)) and x.lower() != "none") else y + "_" + OTHER
FUNC_NONE = lambda x: float(x) if (x and x.lower() != "none") else 0

CATEGORY_URL = None

CONN_REDIS = redis.ConnectionPool(host='localhost', port=6379, db=0)
DB_REDIS = redis.Redis(connection_pool=CONN_REDIS)

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

def norm_url(url):
    start_idx = url.find("?")
    return url[:start_idx if start_idx > -1 else len(url)].replace("http://", "https://")

def _categorized_url(url, otype="all"):
    global CATEGORY_URL, DOMAIN_MAP, FUNC, OTHER

    if CATEGORY_URL is None:
        CATEGORY_URL = load_category()

    n_url = norm_url(url)

    logic1, logic2, function, intention = None, None, None, None
    if n_url in CATEGORY_URL:
        logic1, logic2, function, intention = CATEGORY_URL[n_url]["logic1"], CATEGORY_URL[n_url]["logic2"], CATEGORY_URL[n_url]["function"], CATEGORY_URL[n_url]["intention"]
    else:
        domain = DOMAIN_MAP.get(urlparse(url).netloc, OTHER)
        logic1, logic2, function, intention = domain, domain, domain, domain

    ret = None
    if otype == "all":
        ret = FUNC(logic1, "logic1"), FUNC(logic2, "logic2"), FUNC(function, "function"), FUNC(intention, "intention")
    elif otype == "logic1":
        ret = FUNC(logic1, "logic1")
    elif otype == "logic2":
        ret = FUNC(logic2, "logic2")
    elif otype == "function":
        ret = FUNC(function, "function")
    elif otype == "intention":
        ret = FUNC(intention, "intention")
    elif otype == "logic":
        ret = FUNC(logic1, "logic1") + "_" + FUNC(logic2, "logic2")

    return ret

def _rich_url(logic1, logic2, function, intention):
    return "_".join([logic1, logic2]), "_".join([logic1, function]), "_".join([logic2, function]), "_".join([logic1, intention]), "_".join([logic2, intention])

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

def load_cookie_history():
    global DB_REDIS

    for cookie_id in DB_REDIS.keys():
        yield cookie_id, json.loads(DB_REDIS.get(cookie_id))

def save_cookie_history(results, filepath=FILEPATH_COOKIE_ID):
    with open(filepath, "wb") as out_file:
        pickle.dump(results, out_file)

def create_cookie_history(filepath, pool={}):
    global ENCODE_UTF8, SEP, CONN_REDIS, DB_REDIS

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

                #pool.setdefault(cookie_id, []).append(creation_datetime)
                history = []
                ret = DB_REDIS.get(cookie_id)
                if ret:
                    history = json.loads(ret)

                history.append(creation_datetime.strftime("%Y-%m-%d %H:%M:%S"))
                DB_REDIS.set(cookie_id, json.dumps(history))

    #DB_REDIS.save()

    print("The size of db is {}".format(len(DB_REDIS.keys())))
    print("The key of last record is {}".format(cookie_id))

    return pool

def unknown_urls():
    filepath_raw_page = os.path.join(BASEPATH, "data", "temp", "page_2016-09-01*.tsv.gz")

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

        print(filepath)

    with open("unknown_url.txt.1", "wb") as out_file:
        for url, count in urls.items():
            out_file.write(url.encode(ENCODE_UTF8))
            out_file.write(SEP)
            out_file.write(str(count))
            out_file.write("\n")

if __name__ == "__main__":
    #unknown_urls()

    #print FUNC("https://www.myb2b.com.tw/ebank/ebgetpwdstatus.asp", "logic")

    '''
    pool = set()
    with gzip.open("../data/temp/page_2016-09-01_10.tsv.gz") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                url = parse_raw_page(line)[3]
                pool.add(urlparse(url).netloc)

    for domain in pool:
        print domain
    '''

    df = {}
    filepath_raw_cookie = os.path.join(BASEPATH, "data", "raw", "cookie_[0-9]*.tsv.gz")
    for filepath in sorted(glob.glob(filepath_raw_cookie)):
        if len(os.path.basename(filepath)) > 22:
            df = create_cookie_history(filepath, df)
            print("current filepath is {}".format(filepath))

    save_cookie_history(df)

    '''
    pool = set()
    results = load_cookie_history("../data/setting/cookie_history.pkl.1")
    #for cookie_id, creation_datetime in results.items():
        #print((cookie_id, creation_datetime))
        #pool.add(cookie_id)
    pool = set(results.keys())

    results = set(load_cookie_history().keys())
    for cookie_id in pool - results:
        print cookie_id

    print len(pool)
    print len(results)
    '''
