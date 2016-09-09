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
cookie_id                   4991d0ad940743178e0fcd661da2f82d
individual_id               P22B00DB1B34345B48
creation_datetime           2016-09-01 10:16:19.283000
duration                    26521.0
active_duration             19511.0
loading_duration            8351.0
lifetime                    -- 先忽略
logic                       {"理財": 12, "信貸": 1}
function                    {"登入": 1, "查詢": 2}
intention                   {"旅遊": 1, "有車": 5}
'''

SEP = "\t"

INIT_R = {"cookie_id": None,
          "individual_id": None,
          "duration": 0,
          "active_duration": 0,
          "loading_duration": 0,
          "logic": {},
          "function": {},
          "intention": {},}

def set_record(results, creation_datetime, cookie_id, individual_id, logic, function, intention, duration, active_duration, loading_duration):
    global INIT_R

    results.setdefault(cookie_id, INIT_R.copy())
    # implement your logic


def luigi_run(filepath, results={}):
    global SEP

    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, _, creation_datetime, function, logic, intention, duration, active_duration, loading_duration, _ = line.decode("utf8").strip().split(SEP)

                set_record(results, cookie_id, creation_datetime, individual_id, logic, function, intention, duration, active_duration, loading_duration)

    return results