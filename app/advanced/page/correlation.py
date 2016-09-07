#-*- coding: utf-8 -*-

import time
import numpy as np
import pandas as pd

from pandas import DataFrame

SESSION = 'session_id'
PAGELINK = 'url'
PAGESEQ = 'session_seq'
EVENTTIMESTAMP = 'creation_datetime'


def luigi_run(FILEPATH, chain_length=2, pagedict={}, pagecount={}):
    filepath = FILEPATH

    sessionall = pd.read_csv(filepath, usecols=[SESSION, EVENTTIMESTAMP, PAGESEQ, PAGELINK], sep="\t").sort_values([SESSION, PAGESEQ], ascending=[1,1])

    START = "start"
    EXIT = "exit"
    DL = len(sessionall) - 1

    def norm_page(url):
        end_idx = url.find("?")
        return url[:end_idx if end_idx > -1 else len(url)]

    def next_page(pool, start_page, end_page, score):
        pool.setdefault(start_page, {}).setdefault(end_page, 0)
        pool[start_page][end_page] += score

    def page_count(pool, start_page, score):
        pool.setdefault(start_page, 0)
        pool[start_page] += score

    sessionall[PAGELINK] = sessionall[PAGELINK].apply(norm_page) # 只保留"?"之前的URL

    for count, line in enumerate(sessionall.values):
        session = line[0]
        seq = line[1]
        start_page = line[2]

        if start_page.find("https") == -1:
            continue

        for level in range(0, chain_length):
            score = float(chain_length - level) / chain_length

            if count + level <= DL and session == sessionall[SESSION][count + level]:  # 往後 level 個page為同一個Session
                if seq == 1:  # 網頁第一筆資料 session
                    next_page(pagedict, START, sessionall[PAGELINK][count + level], score)
                    page_count(pagecount, START, score)

                if count + level + 1 <= DL and session == sessionall[SESSION][count + level + 1]:
                    next_page(pagedict, start_page, sessionall[PAGELINK][count + level + 1], score)
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

def get_json(df):
    for start_page, info in df.items():
        for end_page, count in sorted(info.items(), key=operator.itemgetter(1), reverse=True):
            d = {"url_start": start_page,
                 "url_end": end_page,
                 "url_type": self.node_type,
                 "date_type": date_type,
                 "creation_datetime": str(self.interval),
                 "count": count[0],
                 "percentage": count[1],
                 "chain_length": self.length}

            yield d

if __name__ == "__main__":
    Correlation("/Users/yehben/Desktop/Page_expose.txt","/Users/yehben/Desktop/output.csv",3)
