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

COUNT = "count"
FUNCTION = "function"
LOGIC1 = "logic1"
LOGIC2 = "logic2"
INTENTION = "intention"

UNKNOWN = u"未分類"

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

FUNC = lambda x, y: y + "_" + x if (x and (isinstance(x, str) or isinstance(x, unicode)) and x.lower() != "none") else DOMAIN_MAP.get(urlparse(x).netloc, OTHER)
FUNC_NONE = lambda x: float(x) if (x and x.lower() != "none") else 0

CATEGORY_URL = None

CONN_LOGIN_REDIS = redis.ConnectionPool(host='localhost', port=6379, db=0)
DB_LOGIN_REDIS = redis.Redis(connection_pool=CONN_LOGIN_REDIS)

CONN_BEHAVIOR_REDIS = redis.ConnectionPool(host='localhost', port=6380, db=0)
DB_BEHAVIOR_REDIS = redis.Redis(connection_pool=CONN_BEHAVIOR_REDIS)

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

def parse_datetime(creation_datetime):
    if creation_datetime.find(".") > -1:
        creation_datetime = datetime.datetime.strptime(creation_datetime, "%Y-%m-%d %H:%M:%S.%f")
    else:
        creation_datetime = datetime.datetime.strptime(creation_datetime, "%Y-%m-%d %H:%M:%S")

    return creation_datetime

def load_history():
    global DB_LOGIN_REDIS

    for cookie_id in DB_LOGIN_REDIS.keys():
        yield cookie_id, json.loads(DB_LOGIN_REDIS.get(cookie_id))

def load_cookie_history(cookie_id):
    global DB_LOGIN_REDIS

    ret = DB_LOGIN_REDIS.get(cookie_id)
    if ret:
        return json.loads(ret)
    else:
        return None

def save_cookie_history(cookie_id, creation_datetime):
    global DB_LOGIN_REDIS

    DB_LOGIN_REDIS.set(cookie_id, json.dumps([creation_datetime.strftime("%Y-%m-%d %H:%M:%S")]))

def create_cookie_history(filepath):
    global ENCODE_UTF8, CONN_LOGIN_REDIS, DB_LOGIN_REDIS

    with gzip.open(filepath, "rb") as in_file:
        for line in in_file:
            o = json.loads(line.decode(ENCODE_UTF8))
            cookie_id, creation_datetime = o["cookie_id"], parse_datetime(o["creation_datetime"])

            history = []
            ret = DB_LOGIN_REDIS.get(cookie_id)
            if ret:
                history = json.loads(ret)
            history.append(creation_datetime.strftime("%Y-%m-%d %H:%M:%S"))

            DB_LOGIN_REDIS.set(cookie_id, json.dumps(history))

    print("The size of db is {}".format(len(DB_LOGIN_REDIS.keys())))
    print("The key of last record is {}".format(cookie_id))

def load_behavior(key):
    global DB_BEHAVIOR_REDIS

    ret = DB_BEHAVIOR_REDIS.get(key)
    if ret:
        ret = json.loads(ret)

    return ret

def load_behaviors():
    global DB_BEHAVIOR_REDIS

    for key in DB_BEHAVIOR_REDIS.keys():
        yield key, load_behavior(key)

def save_behavior(key, value):
    global DB_BEHAVIOR_REDIS

    DB_BEHAVIOR_REDIS.set(key, value)

def create_behavior(filepath):
    global ENCODE_UTF8, LOGIC1, LOGIC2, FUNCTION, INTENTION, COUNT, UNKNOWN

    results = {}
    with gzip.open(filepath, "rb") as in_file:
        for line in in_file:
            o = json.loads(line.decode(ENCODE_UTF8))
            cookie_id, creation_datetime = o["cookie_id"], parse_datetime(o["creation_datetime"])
            logic1, logic2, function, intention = o[LOGIC1], o[LOGIC2], o[FUNCTION], o[INTENTION]

            history = load_cookie_history(cookie_id)
            if history:
                for idx, login_datetime in enumerate([datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S") for d in history]):
                    if creation_datetime == login_datetime:
                        key = "TIME_{}".format(idx+1)

                        results.setdefault(key, {COUNT: 0, LOGIC1: {}, LOGIC2: {}, FUNCTION: {}, INTENTION: {}})

                        for subkey, values in zip([LOGIC1, LOGIC2, FUNCTION, INTENTION], [logic1, logic2, function, intention]):
                            tc, total_count = 0, 0
                            for name, value in values.items():
                                if UNKNOWN not in name:
                                    total_count += value

                                tc += value

                            print key, subkey, name, tc, total_count

                            if total_count > 0:
                                for name, value in values.items():
                                    name = name.replace(" ", "").replace(u"投資理財", u"理財投資")
                                    results[key][subkey].setdefault(name, 0)
                                    results[key][subkey][name] += float(value) / total_count

                        results[key][COUNT] += 1

                        break

            else:
                print("Not found {} in 'login' database".format(cookie_id))

    print("finish {} with {}".format(filepath, results.keys()))

    for key, values in results.items():
        ret = load_behavior(key)
        if ret:
            for subkey, category in values.items():
                if isinstance(category, int):
                    ret[subkey] += category
                else:
                    for name, value in category.items():
                        ret[subkey].setdefault(name, 0)
                        ret[subkey][name] += value

            print("update {}".format(key))
            save_behavior(key, json.dumps(ret))
        else:
            print("not found {}".format(key))
            save_behavior(key, json.dumps(values))

def unknown_urls():
    filepath_raw_page = os.path.join(BASEPATH, "data", "temp", "page_2016-09-21*.tsv.gz")

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

    with open("unknown_url.txt", "wb") as out_file:
        for url, count in urls.items():
            out_file.write(url.encode(ENCODE_UTF8))
            out_file.write(SEP)
            out_file.write(str(count))
            out_file.write("\n")

if __name__ == "__main__":
    '''
    # Create the login_datetime database

    filepath_raw_cookie = os.path.join(BASEPATH, "data", "raw", "cookie_[0-9]*.tsv.gz")
    for filepath in sorted(glob.glob(filepath_raw_cookie)):
        if len(os.path.basename(filepath)) > 22:
            create_cookie_history(filepath)
            print("current filepath is {}".format(filepath))
    '''

    for filepath in glob.glob(os.path.join(BASEPATH_RAW, "cookie_*.tsv.gz")):
        print("Start to proceed {}".format(filepath))
        create_behavior(filepath)

        break
