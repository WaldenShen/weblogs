#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
import json

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
================================
cookie_key          logic / function / intention
cookie_value        # Logic: 理財/投資/信貸... Intention: 旅遊/美食/...
date_type           hour / day / week / month / year
creation_datetime   2016-09-01 10:16:19.283000
n_count             10
'''

SEP = "\t"
ENCODE = "utf8"
OTHER = "其他"

INIT_R = {"cookie_key": None,
          "cookie_value": None,
          "n_count": 0}

def luigi_run(filepath, results={}):
    global SEP, ENCODE

    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, url, _, function, logic, intention, duration, active_duration, loading_duration, _ = line.decode(ENCODE).strip().split(SEP)

                logic = logic if (logic and logic.lower() != "none") else OTHER
                key = "logic_" + logic
                results.setdefault(key, INIT_R.copy())
                results[key]["cookie_key"] = "logic"
                results[key]["cookie_value"] = logic
                results[key]["n_count"] += 1

                function = function if (function and function.lower() != "none") else OTHER
                key = "function_" + function
                results.setdefault(key, INIT_R.copy())
                results[key]["cookie_key"] = "function"
                results[key]["cookie_value"] = function
                results[key]["n_count"] += 1

                intention = intention if (intention and intention.lower() != "none") else OTHER
                key = "intention_" + intention
                results.setdefault(key, INIT_R.copy())
                results[key]["cookie_key"] = "intention"
                results[key]["cookie_value"] = intention
                results[key]["n_count"] += 1

    return results

def luigi_dump(out_file, results, creation_datetime, date_type):
    for d in results.values():
        d["creation_datetime"] = creation_datetime
        d["date_type"] = date_type

        out_file.write("{}\n".format(json.dumps(d)))
