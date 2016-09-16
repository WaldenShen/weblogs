#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
import json

from utils import is_app_log
from utils import SEP, ENCODE_UTF8, OTHER, FUNC

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


def luigi_run(filepath, filter_app=False, results={}):
    global SEP, ENCODE_UTF8, OTHER, FUNC

    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, url, _, function, logic, intention, duration, active_duration, loading_duration, _ = line.decode(ENCODE_UTF8).strip().split(SEP)

                if filter_app and is_app_log(url):
                    continue

                logic = FUNC(logic, "logic")
                function = FUNC(function, "function")
                intention = FUNC(intention, "intention")

                results.setdefault(cookie_id, {})

                for name, value in zip(["logic", "function", "intention"], [logic, function, intention]):
                    key = name + "_" + value
                    results[cookie_id].setdefault(key, 0)
                    results[cookie_id][key] += 1

    return results

def luigi_dump(out_file, results, creation_datetime, date_type):
    r = {}

    for cookie_id, info in results.items():
        total_count = 0

        for key, value in info.items():
            r.setdefault(key, {})
            category_key, category_value = key.split("_", 1)

            r[key]["category_key"] = category_key
            r[key]["category_value"] = category_value
            r[key].setdefault("n_count", 0)

            if key.find("logic") > -1:
                total_count += value

        for key, value in info.items():
            r[key]["n_count"] += float(value) / total_count

    for d in r.values():
        global ENCODE_UTF8

        d["creation_datetime"] = creation_datetime
        d["date_type"] = date_type

        try:
            out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
        except:
            out_file.write("{}\n".format(json.dumps(d)))
