#!/usr/bin/python

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
domain
page_view
user_view
chain_length
duration
active_duration
loading_duration
count_failed
count_session
count_cookie
count_logic         DICT
count_function      DICT
count_intention     DICT
count_uncategory    INT
'''

SEP = "\t"

INIT_R = {"domain": None,
          "page_view": 0,
          "user_view": 0,
          "chain_length": 0,
          "duration": 0,
          "active_duration": 0,
          "loading_duration": 0,
          "count_failed": 0,
          "count_session": 0,
          "count_cookie": 0,
          "count_logic": {},
          "count_function": {},
          "count_intention": {},
          "count_uncategory": 0}

def luigi_run(filepath, results={}):
    global SEP

    results = {"domain": None,
          "page_view": 0,
          "user_view": 0,
          "chain_length": 0,
          "duration": 0,
          "active_duration": 0,
          "loading_duration": 0,
          "count_failed": 0,
          "count_session": 0,
          "count_cookie": 0,
          "count_logic": {},
          "count_function": {},
          "count_intention": {},
          "count_uncategory": 0}

    with gzip.open(filepath, "r", encoding="utf-8") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, cookie_id, individual_id, _, _. _, function, logic, intention, duration, active_duration, loading_duration, _ = line.strip().split(SEP)

                # implement your logic

    return results
