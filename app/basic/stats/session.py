#!/usr/bin/python

import gzip

'''
INPUT (JSON Format)
============================
session_id
cookie_id
individual_id
creation_datetime
duration
active_duration
loading_duration
chain_length
logic
function
intention

OUTPUT
============================
category            logic / function / intention
category_value      信用卡, 信貸... / 登入, 輸入... / 旅遊, 美食...
date_type           hour / day / week / month / year
creation_datetime   2016-09-01 10:16:19.283000
duration            92525.0
active_duration     5190.0
loading_duration    10935.3
n_count             10
'''

SEP = "\t"
INIT_R = {"category": None,
          "category_value": None,
          "date_type": None,
          "creation_datetime": None,
          "duration": 0,
          "active_duration": 0,
          "loading_duration": 0,
          "chain_length": 0,
          "n_count": 0}

def set_record(results, o_json):
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
                o_json = json.loads(line.strip())

                set_record(results, o_json)

    return results
