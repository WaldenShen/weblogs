#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip

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

SEP = "\t"
INIT_R = {"url": None,
          "url_type": None,
          "date_type": None,
          "creation_datetime": None,
          "page_view": 0,
          "user_view": 0,
          "duration": 0,
          "active_duration": 0,
          "loading_duration": 0,
          "n_count": 0}

def set_record(results, session_id, cookie_id, individual_id, logic, function, intention, duration, active_duration, loading_duration):
    global INIT_R

    results.setdefault(session_id, INIT_R.copy())
    # implement your logic

def luigi_run(filepath, results={}):
    global SEP

    with gzip.open(filepath, "r", encoding="utf-8") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, _. _, function, logic, intention, duration, active_duration, loading_duration, _ = line.strip().split(SEP)

                set_record(results, session_id, cookie_id, individual_id, logic, function, intention, duration, active_duration, loading_duration)

    return results
