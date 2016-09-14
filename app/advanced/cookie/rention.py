#!/usr/bin/python
#-*- coding: utf-8 -*-

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

    total_count = len(df)

    results = {"creation_datetime": creation_datetime}
    for cookie_id, values in df.items():
        if len(values) == 2:
            diff = math.ceil(float((values[1] - values[0]).total_seconds()) / 86400)

            if diff == 0:
                print((cookie_id, values, (values[1] - values[0]).total_seconds()))

            key = None
            if diff <= 7:
                key = "return_{}".format(diff)
            elif diff <= 14:
                key = "return_14"
            elif diff <= 21:
                key = "return_21"
            else:
                key = "return_28"

            results.setdefault(key, 0)
            results[key] += 1

    for k, v in results.items():
        if k.find("return") > -1:
            results[k] = float(v) / total_count

    out_file.write(bytes("{}\n".format(json.dumps(results)), ENCODE_UTF8))
