#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
import json

from utils import norm_url
from utils import SEP, OTHER, ENCODE_UTF8, FUNC, FUNC_NONE

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
user_viwe           734
duration            92525.0
active_duration     51930.0
loading_duration    10935.3
'''

def set_record(results, cookie_id, url, logic, function, intention, duration, active_duration, loading_duration):
    global OTHER, FUNC

    logic = FUNC(logic, "logic")
    function = FUNC(function, "function")
    intention = FUNC(intention, "intention")

    for key_type, key in zip(["url", "logic", "function", "intention"], [url, logic, function, intention]):
        # implement your logic
        init_r = {"url": None,
                  "url_type": None,
                  "page_view": 0,
                  "user_view": set(),
                  "duration": 0,
                  "active_duration": 0,
                  "loading_duration": 0}

        results.setdefault(key, init_r)

        # implement your logic


def luigi_run(filepath, results={}):
    global SEP

    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, url, creation_datetime, function, logic, intention, duration, active_duration, loading_duration, _ = line.decode(ENCODE_UTF8).strip().split(SEP)

                set_record(results, cookie_id, norm_url(url), logic, function, intention, duration, active_duration, loading_duration)

    return results

def luigi_dump(out_file, results, creation_datetime, date_type):
    for d in results.values():
        d["creation_datetime"] = creation_datetime
        d["date_type"] = date_type

        d["user_view"] = len(d["user_view"])

        out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
