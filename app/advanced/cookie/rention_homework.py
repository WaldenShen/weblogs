#!/usr/bin/python
#-*- coding: utf-8 -*-

import gzip
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

                # implement your logic


    return results

def luigi_dump(out_file, df, creation_datetime):
    global ENCODE_UTF8

    # implement your logic

    out_file.write(bytes("{}\n".format(json.dumps(results)), ENCODE_UTF8))

if __name__ == "__main__":
    import glob

    df = {}
    for f in glob.iglob("../data/raw/cookie_2016-08-*gz"):
        print(f)
        df = luigi_run(f, df)

    for cookie_id, values in df.items():
        print((cookie_id, values))
