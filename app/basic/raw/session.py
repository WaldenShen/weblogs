#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
import json

from utils import SEP, ENCODE_UTF8, OTHER

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
session_id              35651589
cookie_id               4991d0ad940743178e0fcd661da2f82d
individual_id           P22B00DB1B34345B48
creation_datetime       2016-09-01 10:16:19.283000
duration                83695.0
active_duration         29212.0
chain_length            27
logic                   {"理財": 12, "信貸": 1}
function                {"登入": 1, "查詢": 2}
intention               {"旅遊": 1, "有車": 5}
count_event             -- 先忽略
'''

INIT_R = {"session_id": None,
          "cookie_id": None,
          "individual_id": None,
          "duration": 0,
          "active_duration": 0,
          "loading_duration": 0,
          "chain_length": 0,
          "logic": {},
          "function": {},
          "intention": {},
          "count_event": 0}

def set_record(results, session_id, cookie_id, individual_id, creation_datetime, logic, function, intention, duration, active_duration, loading_duration):
    global INIT_R, OTHER

    results.setdefault(session_id, INIT_R.copy())

    results.setdefault(session_id, INIT_R.copy())
    results[session_id]["session_id"] = session_id
    results[session_id]["cookie_id"] = cookie_id
    results[session_id]["individual_id"] = individual_id
    results[session_id]["creation_datetime"] = creation_datetime

    results[session_id]["duration"] += float(duration)
    results[session_id]["active_duration"] += float(active_duration)
    results[session_id]["loading_duration"] += float(active_duration)

    results[session_id]["chain_length"] += 1

    logic = logic if (logic and logic.lower() != "none") else OTHER
    results[session_id]["logic"].setdefault(logic, 0)
    results[session_id]["logic"][logic] += 1

    function = function if (function and function.lower() != "none") else OTHER
    results[session_id]["function"].setdefault(function, 0)
    results[session_id]["function"][function] += 1

    intention = intention if (intention and intention.lower() != "none") else OTHER
    results[session_id]["intention"].setdefault(intention, 0)
    results[session_id]["intention"][intention] += 1

def luigi_run(filepath, results={}):
    global SEP, ENCODE_UTF8

    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, _, creation_datetime, function, logic, intention, duration, active_duration, loading_duration, _ = line.decode(ENCODE_UTF8).strip().split(SEP)

                set_record(results, session_id, cookie_id, individual_id, creation_datetime, logic, function, intention, duration, active_duration, loading_duration)

    return results

def luigi_dump(out_file, results, creation_datetime, date_type):
    global ENCODE_UTF8

    for d in results.values():
        out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
