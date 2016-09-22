#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import gzip
import math
import json
import redis
import datetime

from utils import SEP, ENCODE_UTF8
from utils import load_cookie_history

'''
INPUT (schema JSON format)
============================
cookie_id                   4991d0ad940743178e0fcd661da2f82d
individual_id               P22B00DB1B34345B48
creation_datetime           2016-09-01 10:16:19.283000
duration                    26521.0
active_duration             19511.0
loading_duration            8351.0
lifetime                    -- 先忽略
logic1                      {"理財": 12, "信貸": 1}
logic2
function                    {"登入": 1, "查詢": 2}
intention                   {"旅遊": 1, "有車": 5}


OUTPUT
============================
login_datetime      2016-09-01
creation_datetime   2016-09-12
return_1            0.3424
return_2            0.2525
return_3            0.2142
return_4            ...
return_5            ...
return_6
return_7
return_14
return_21
return_28
return_56
no_return
'''

def luigi_run(date_start, results={}):
    for cookie_id, dates in load_cookie_history():
        first_login = datetime.datetime.strptime(dates[0], "%Y-%m-%d %H:%M:%S")
        if first_login >= date_start:
            second_login = None

            if len(dates) > 1:
                second_login = datetime.datetime.strptime(dates[1], "%Y-%m-%d %H:%M:%S")
                results.setdefault(cookie_id, [first_login, second_login])
            else:
                results.setdefault(cookie_id, [first_login])

    print date_start, len(results)

    return results

def luigi_dump(out_file, df, creation_datetime, date_type):
    global ENCODE_UTF8

    results = {}
    for cookie_id, dates in df.items():
        date_key = dates[0].strftime("%Y-%m-%d")
        results.setdefault(date_key, {"login_datetime": date_key,
                                      "creation_datetime": creation_datetime,
                                      "return_1": 0,
                                      "return_2": 0,
                                      "return_3": 0,
                                      "return_4": 0,
                                      "return_5": 0,
                                      "return_6": 0,
                                      "return_7": 0,
                                      "return_14": 0,
                                      "return_21": 0,
                                      "return_28": 0,
                                      "return_56": 0,
                                      "no_return": 0})

        if len(dates) == 1:
            results[date_key]["no_return"] += 1
        else:
            diff = int(math.ceil(float((dates[1] - dates[0]).total_seconds()) / 86400))

            key = None
            if diff <= 0:
                pass
            elif diff <= 7:
                key = "return_{}".format(diff)
            elif diff <= 14:
                key = "return_14"
            elif diff <= 21:
                key = "return_21"
            elif diff <= 28:
                key = "return_28"
            elif diff <= 56:
                key = "return_56"
            else:
                key = "no_return"

            results[date_key][key] += 1

    for value in results.values():
        try:
            out_file.write(bytes("{}\n".format(json.dumps(value)), ENCODE_UTF8))
        except:
            out_file.write("{}\n".format(json.dumps(value)))

if __name__ == "__main__":
    import glob

    df = {}
    for fn in sorted(glob.glob("../data/raw/cookie_2016-08-0*.tsv.gz")):
        if fn.find("01") > -1 or fn.find("02") > -1 or fn.find("03") > -1 or fn.find("04") > -1:
            df = luigi_run(fn, df)

    out_file = gzip.open("tt.tsv.gz", "wb")
    luigi_dump(out_file, df, "2016-08-03", "day")
    #for cookie_id, dates in df.items():
    #    date_key = dates[0].strftime("%Y-%m-%d")
