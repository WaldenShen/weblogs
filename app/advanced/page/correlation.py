#!/usr/bin/python
#-*- coding: utf-8 -*-

import time
import operator
import numpy as np
import pandas as pd

from pandas import DataFrame
from utils import norm_url, FUNC, SEP

SESSION = 'session_id'
PAGELINK = 'url'
PAGESEQ = 'session_seq'
EVENTTIMESTAMP = 'creation_datetime'


def luigi_run(FILEPATH, node_type=PAGELINK, chain_length=2, pagedict={}, pagecount={}):
    filepath = FILEPATH

    sessionall = pd.read_csv(filepath, usecols=[SESSION, EVENTTIMESTAMP, PAGESEQ, node_type], sep=SEP).sort_values([SESSION, PAGESEQ], ascending=[1,1])

    START = "start"
    EXIT = "exit"
    DL = len(sessionall) - 1

    def next_page(pool, start_page, end_page, score):
        pool.setdefault(start_page, {}).setdefault(end_page, 0)
        pool[start_page][end_page] += score

    def page_count(pool, start_page, score):
        pool.setdefault(start_page, 0)
        pool[start_page] += score

    if node_type == PAGELINK:
        sessionall[node_type] = sessionall[node_type].apply(norm_url) # 只保留"?"之前的URL
    elif node_type == "logic":
        sessionall[node_type] = sessionall[node_type].apply(FUNC, args=("logic",))
    elif node_type == "function":
        sessionall[node_type] = sessionall[node_type].apply(FUNC, args=("function",))
    elif node_type == "intention":
        sessionall[node_type] = sessionall[node_type].apply(FUNC, args=("intention",))

    for count, line in enumerate(sessionall.values):
        session = line[0]
        seq = line[1]

        start_page = line[2]
        if node_type != PAGELINK:
            start_page = line[3]

        if start_page.find("app") > -1:
            continue

        for level in range(0, chain_length):
            score = float(chain_length - level) / chain_length

            if count + level <= DL and session == sessionall[SESSION][count + level]:  # 往後 level 個page為同一個Session
                if seq == 1:  # 網頁第一筆資料 session
                    next_page(pagedict, START, sessionall[node_type][count + level], score)
                    page_count(pagecount, START, score)

                if count + level + 1 <= DL and session == sessionall[SESSION][count + level + 1]:
                    next_page(pagedict, start_page, sessionall[node_type][count + level + 1], score)
                    page_count(pagecount, start_page, score)
                else:
                    next_page(pagedict, start_page, EXIT, score)
                    page_count(pagecount, start_page, score)

                    break
            else:
                next_page(pagedict, start_page, EXIT, score)
                page_count(pagecount, start_page, score)

                break  # 只要到EXIT後就不繼續level

    pagecount.setdefault(EXIT, 0)

    pageall = pagecount.keys()

    results = {}
    for start_page, exit_pages in pagedict.items():
        for exit_page, count in exit_pages.items():
            if count > 0:
                results.setdefault(start_page, {}).setdefault(exit_page, [count, float(count) / pagecount[start_page]])

    return results

def get_json(df, node_type, date_type, interval, length):
    for start_page, info in df.items():
        for end_page, count in sorted(info.items(), key=operator.itemgetter(1), reverse=True):
            d = {"url_start": start_page,
                 "url_end": end_page,
                 "url_type": node_type,
                 "date_type": date_type,
                 "creation_datetime": interval,
                 "n_count": count[0],
                 "percentage": count[1],
                 "chain_length": length}

            yield d

if __name__ == "__main__":
    length = 4
    node_type = "intention"
    filepath = "../data/temp/page_2016-09-01_10.csv.gz"

    df = luigi_run(filepath, node_type, length)

    for d in get_json(df, node_type, "hour", "2016-09-01", length):
        print(d)
