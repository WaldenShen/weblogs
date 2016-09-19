#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
import json

from utils import parse_raw_page
from utils import ENCODE_UTF8

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
cookie_id                   4991d0ad940743178e0fcd661da2f82d
individual_id               P22B00DB1B34345B48
creation_datetime           2016-09-01 10:16:19.283000
duration                    26521.0
active_duration             19511.0
loading_duration            8351.0
lifetime                    -- 先忽略
logic1                      {"理財": 12, "信貸": 1}
logic2
function                    {"登入": 1, "查詢": 2}
intention                   {"旅遊": 1, "有車": 5}
'''


def set_record(results, creation_datetime, cookie_id, individual_id, logic1, logic2, function, intention, duration, active_duration, loading_duration):
    init_r = {"cookie_id": None,
              "individual_id": None,
              "creation_datetime": None,
              "duration": 0,
              "active_duration": 0,
              "loading_duration": 0,
              "logic1": {},
              "logic2": {},
              "function": {},
              "intention": {},}

    results.setdefault(cookie_id, init_r)

    results[cookie_id]["cookie_id"] = cookie_id
    results[cookie_id]["individual_id"] = individual_id

    results[cookie_id]["creation_datetime"] = creation_datetime

    results[cookie_id]["duration"] += duration
    results[cookie_id]["active_duration"] += active_duration
    results[cookie_id]["loading_duration"] += loading_duration

    for key, value zip(["logic1", "logic2", "function", "intention"], [logic1, logic2, function, intention]):
        results[cookie_id][key].setdefault(value, 0)
        results[cookie_id][key][value] += 1

def luigi_run(filepath, is_first, results={}):
    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, url, creation_datetime,\
                logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention,\
                duration, active_duration, loading_duration, _ = parse_raw_page(line)

                set_record(results, creation_datetime, cookie_id, individual_id, logic, function, intention, duration, active_duration, loading_duration)

    return results

def luigi_dump(out_file, df, creation_datetime, date_type):
    global ENCODE_UTF8

    for d in df.values():
        try:
            out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
        except:
            out_file.write("{}\n".format(json.dumps(d)))
