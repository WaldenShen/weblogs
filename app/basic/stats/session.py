#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
import json

from utils import is_app_log, parse_raw_page
from utils import ENCODE_UTF8, DOMAIN_MAP

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
category_key          logic1 / logic2 / function / intention
category_value        # Logic: 理財/投資/信貸... Intention: 旅遊/美食/...
date_type             hour / day / week / month / year
creation_datetime     2016-09-01 10:16:19.283000
n_count               10
'''


def luigi_run(filepath, filter_app=False, results={}):
    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        pre_session_id, pre_total_count, piece = None, 0, {}

        for line in in_file:
            if is_header:
                is_header = False
            else:
                info = parse_raw_page(line)

                if info:
                    session_id, cookie_id, individual_id, url, creation_datetime,\
                    logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention,\
                    duration, active_duration, loading_duration = parse_raw_page(line)

                    if filter_app and is_app_log(url):
                        continue

                    for name, value in zip(["logic1", "logic2", "function", "intention", "logic", "logic1_function", "logic2_function", "logic1_intention", "logic2_intention"], [logic1, logic2, function , intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention]):
                        key = name + "_" + value

                        init_r = {"category_key": None,
                                  "category_value": None,
                                  "n_count": 0}

                        results.setdefault(key, init_r)
                        results[key]["category_key"] = name
                        results[key]["category_value"] = value

                    if pre_session_id is not None and pre_session_id != session_id:
                        for key, info in piece.items():
                            results[key]["n_count"] += float(info["n_count"]) / pre_total_count

                        pre_total_count = 0
                        piece = {}

                    for name, value in zip(["logic1", "logic2", "function", "intention", "logic", "logic1_function", "logic2_function", "logic1_intention", "logic2_intention"], [logic1, logic2, function , intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention]):
                        key = name + "_" + value

                        init_r = {"category_key": None,
                                  "category_value": None,
                                  "n_count": 0}

                        piece.setdefault(key, init_r)
                        piece[key]["category_key"] = name
                        piece[key]["category_value"] = value
                        piece[key]["n_count"] += 1

                    pre_session_id = session_id
                    pre_total_count += 1

    for key, info in piece.items():
        results[key]["n_count"] += float(info["n_count"]) / pre_total_count

    return results

def luigi_dump(out_file, results, creation_datetime, date_type):
    global ENCODE_UTF8

    for d in results.values():
        d["creation_datetime"] = creation_datetime
        d["date_type"] = date_type

        try:
            out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
        except:
            out_file.write("{}\n".format(json.dumps(d)))
