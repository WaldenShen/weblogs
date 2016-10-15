#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import gzip
import json
import datetime

from utils import ENCODE_UTF8, UNKNOWN, ALL_CATEGORIES
from utils import LOGIC1, LOGIC2, FUNCTION, INTENTION

'''
INPUT (schema JSON format)
============================
{"creation_datetime": "2016-09-21 22:35:26", "category_key": "cub\u5e73\u53f0", "prev_interval": 247346.0, "individual_id": "A1BF7465FD4ACD0B1D", "total_count2": 8, "total_count1": 8, "first_interval": 9707258.0, "cookie_id": "d2284f777b084c1ba6bb9065a96bdf75", "times": 26, "category_value": 6, "category_type": "logic1"}

individual_id
cookie_id
category_type
category_key
category_value
total_count1
total_count2
first_interval
prev_intention

OUTPUT
============================
cookie_id
creation_datetime
interval_logic1
interval_logic2
interval_function
interval_intention
interval_logic
interval_logic1function
interval_logic2function
interval_logic1intention
interval_logic2intention
'''

def luigi_run(filepath, results={}):
    global ENCODE_UTF8, LOGIC1, LOGIC2, FUNCTION, INTENTION, ALL_CATEGORIES

    with gzip.open(filepath, "rb") as in_file:
        for line in in_file:
            o = json.loads(line.decode(ENCODE_UTF8).strip())
            cookie_id, total_count2 = o["cookie_id"], o["total_count2"]
            category_type = o["category_type"]
            logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention = \
                o[LOGIC1], o[LOGIC2], o[FUNCTION], o[INTENTION], o["logic"], o["logic1_function"], o["logic2_function"], o["logic1_intention"], o["logic2_intention"]
            prev_interval = o["pre_interval"]

            if total_count2 > 0 and pre_interal > 0:
                results.setdefault(cookie_id, {LOGIC1: 0,
                                               LOGIC2: 0,
                                               FUNCTION: 0,
                                               INTENTION: 0,
                                               "logic": 0,
                                               "logic1_intention": 0,
                                               "logic2_intention": 0,
                                               "n_count": 0})

                max_name, max_value = None, -sys.maxint
                for name, value in zip(ALL_CATEGORIES, [logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention]):
                    if UNKNOWN not in value:
                        if value > max_value:
                            max_name, max_value = name, float(value) / total_count2

                if max_name:
                    results[cookie_id][max_name] += interval


def luigi_dump(out_file, df, creation_datetime, date_type):
    pass

if __name__ == "__main__":
    import glob

    df = {}
    for fn in sorted(glob.glob("../data/raw/cookie_2016-08-0*.tsv.gz")):
        if fn.find("01") > -1 or fn.find("02") > -1 or fn.find("03") > -1 or fn.find("04") > -1:
            df = luigi_run(fn, df)

