#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import gzip
import math
import json
import datetime

from utils import SEP, ENCODE_UTF8

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
logic                       {"理財": 12, "信貸": 1}
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
no_return
'''


def luigi_run(filepath, results={}):
    global SEP, ENCODE_UTF8

    with gzip.open(filepath, "rb") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                o = json.loads(line.strip().decode(ENCODE_UTF8))

                cookie_id = o["cookie_id"]
                creation_datetime = None
                if o["creation_datetime"].find(".") > -1:
                    creation_datetime = datetime.datetime.strptime(o["creation_datetime"], "%Y-%m-%d %H:%M:%S.%f")
                else:
                    creation_datetime = datetime.datetime.strptime(o["creation_datetime"], "%Y-%m-%d %H:%M:%S")

                results.setdefault(cookie_id, [])
                if len(results[cookie_id]) < 2:
                    results[cookie_id].append(creation_datetime)

    return results

def luigi_dump(out_file, df, creation_datetime, date_type):
    global ENCODE_UTF8

    results = {}
    for cookie_id, dates in df.items():
        results.setdefault(dates[0].strftime("%Y-%m-%d"), {"login_datetime": dates[0].strftime("%Y-%m-%d"),
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
                                                           "no_return": 0})

        if len(dates) == 1:
            results[dates[0].strftime("%Y-%m-%d")]["no_return"] += 1
        else:
            diff = math.ceil(float((dates[1] - dates[0]).total_seconds()) / 86400)

            key = None
            if diff <= 7:
                key = "return_{}".format(diff)
            elif diff <= 14:
                key = "return_14"
            elif diff <= 21:
                key = "return_21"
            else:
                key = "return_28"

            results[dates[0].strftime("%Y-%m-%d")][key] += 1

    for value in results.values():
        out_file.write(bytes("{}\n".format(json.dumps(value)), ENCODE_UTF8))

if __name__ == "__main__":
    import glob

    df = {}
    for fn in sorted(glob.glob("../data/raw/cookie_2016-08-*.tsv.gz")):
        df = luigi_run(fn, df)

    try:
        out_file.write(bytes("{}\n".format(json.dumps(results)), ENCODE_UTF8))
    except:
        out_file.write("{}\n".format(json.dumps(results)))

    with gzip.open("tt.tsv.gz", "wb") as out_file:
        luigi_dump(out_file, df, "2016-08-01", "day")
