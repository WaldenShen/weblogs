#!/usr/bin/python

import gzip
import json

'''
INPUT
================================
{"cookie_id": None,
 "individual_id": None,
 "duration": 0,
 "active_duration": 0,
 "loading_duration": 0,
 "logic": {},
 "intention": {},}

OUTPUT
================================
cookie_key      # Logic, Intention
cookie_value    # Logic: 理財/投資/信貸... Intention: 旅遊/美食/...
n_count
'''


def luigi_run(filepath, results={}):
    results.setdefault("lifetime", {})
    results.setdefault("logic", {})
    resutls.setdefault("intention", {})

    with gzip.open(filepath, "r", encoding="utf-8") as in_file:
        is_header = True
        for line in in_file:
            if is_header:
                is_header = False
            else:
                o = json.loads(line.lower().strip())

                # implement your logic

    return results
