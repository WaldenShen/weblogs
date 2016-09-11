#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
import json

try
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

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
============================
domain              https://www.cathaybk.com.tw
date_type           hour / daily / week / month / year
creation_datetime   2016-09-01 10:00:00 / 2016-09-01 / 2016-W34 / 2016-09 / 2016
page_view           19734
user_view           2712
duration            3153021.0
active_duration     1935515.0
loading_duration    937515.0
count_failed        -- 先忽略
count_session       253515
count_cookie        193152
count_logic         {"理財": 12, "信貸": 1}
count_function      {"登入": 1, "查詢": 2}
count_intention     {"旅遊": 1, "有車": 5}
'''

def luigi_run(filepath, results={}):
    global SEP, OTHER, ENCODE_UTF8, INIT_R, FUNC

    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        session = None

        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, url, _, function, logic, intention, duration, active_duration, loading_duration, _ = line.decode(ENCODE_UTF8).strip().split(SEP)
                website = urlparse(url).netloc

                for domain in [website, "all"]:
                    init_r = {"domain": None,
                              "page_view": 0,
                              "user_view": set(),
                              "duration": 0,
                              "active_duration": 0,
                              "loading_duration": 0,
                              "count_failed": 0,
                              "count_session": 0,
                              "count_logic": {},
                              "count_function": {},
                              "count_intention": {}}

                    results.setdefault(domain, init_r)

                    results[domain]["domain"] = domain
                    results[domain]["page_view"] += 1
                    results[domain]["user_view"].add(cookie_id)

                    results[domain]["duration"] += float(duration)
                    results[domain]["active_duration"] += float(active_duration)
                    results[domain]["loading_duration"] += float(loading_duration)

                    if session_id != session:
                        results[domain]["count_session"] += 1

                    logic = FUNC(logic, "logic")
                    function = FUNC(function, "function")
                    intention = FUNC(intention, "intention")

                    results[domain]["count_logic"].setdefault(logic, 0)
                    results[domain]["count_logic"][logic] += 1

                    results[domain]["count_function"].setdefault(function, 0)
                    results[domain]["count_function"][function] += 1

                    results[domain]["count_intention"].setdefault(intention, 0)
                    results[domain]["count_intention"][intention] += 1

                session = session_id

    return results

def luigi_dump(out_file, results, creation_datetime, date_type):
    global ENCODE_UTF8

    for d in results.values():
        d["creation_datetime"] = creation_datetime
        d["date_type"] = date_type

        d["user_view"] = len(d["user_view"])

        d["count_logic"] = json.dumps(d["count_logic"])
        d["count_function"] = json.dumps(d["count_function"])
        d["count_intention"] = json.dumps(d["count_intention"])

        out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
