#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
import json

from utils import norm_url, is_app_log, parse_raw_page
from utils import OTHER, ENCODE_UTF8

'''
INPUT
============================
session_id      cookie_id       individual_id   session_seq     url     creation_datetime       function        logic   intention       duration    active_duration     loading_time    ip
35651589        4991d0ad940743178e0fcd661da2f82d        P22B00DB1B34345B48      3       https://www.cathaybk.com.tw/cathaybk/   2016-09-01 10:16:19.283000  None    None    None    2218    2218    -1      ip
35651589        4991d0ad940743178e0fcd661da2f82d        P22B00DB1B34345B48      4       https://www.cathaybk.com.tw/cathaybk/exchange/currency-billboard.asp#current        2016-09-01 10:16:21.533000      None    None    None    44679   44679   594     ip
...
...
...

OUTPUT
============================
url                 https://www.cathaybk.com.tw/card/card/index.aspx / 信貸 / 填表 / 旅遊
url_type            url / logic / function / intention
date_type           hour / day / week / month / year
creation_datetime   2016-09-01 10:00:00 / 2016-09-01 / 2016-W34 / 2016-09 / 2016
page_view           1242
user_view           734
profile_view        12
duration            92525.0
active_duration     51930.0
loading_duration    10935.3
'''

def set_record(results, cookie_id, profile_id, url, logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention, duration, active_duration, loading_duration):
    global OTHER

    for key_type, key in zip(["url", "logic1", "logic2", "function", "intention", "logic", "logic1_function", "logic2_function", "logic1_intention", "logic2_intention"], [url, logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention]):
        # implement your logic
        init_r = {"url": None,
                  "url_type": None,
                  "page_view": 0,
                  "user_view": set(),
                  "profile_view": set(),
                  "duration": 0,
                  "active_duration": 0,
                  "loading_duration": 0}

        results.setdefault(key, init_r)

        results[key]["url"] = key
        results[key]["url_type"] = key_type
        results[key]["page_view"] += 1
        results[key]["user_view"].add(cookie_id)

        if profile_id.lower() != "none":
            results[key]["profile_view"].add(profile_id)

        results[key]["duration"] += duration
        results[key]["active_duration"] += active_duration
        results[key]["loading_duration"] += loading_duration

def luigi_run(filepath, filter_app=False, results={}):
    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, url, creation_datetime,\
                logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention,\
                duration, active_duration, loading_duration = parse_raw_page(line)

                if filter_app and is_app_log(url):
                    continue

                set_record(results, cookie_id, individual_id, norm_url(url), logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention, duration, active_duration, loading_duration)

    return results

def luigi_dump(out_file, results, creation_datetime, date_type):
    for d in results.values():
        d["creation_datetime"] = creation_datetime
        d["date_type"] = date_type

        d["user_view"] = len(d["user_view"])
        d["profile_view"] = len(d["profile_view"])

        try:
            out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
        except:
            out_file.write("{}\n".format(json.dumps(d)))

if __name__ == "__main__":
    filepath = "../data/temp/page_2016-09-01_10.csv.gz"
    luigi_run(filepath)
