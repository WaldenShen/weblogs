#-*- coding: utf-8 -*-

import time
import numpy as np
import pandas as pd

from pandas import DataFrame


def luigi_run(FILEPATH, LEVEL=2, pagedict={}, pagecount={}):
    filepath = FILEPATH

    SESSION = 'SessionNumber'
    PAGELINK = 'PageLocation'
    PAGESEQ = 'PageSequenceInSession'
    EVENTTIMESTAMP = 'EventTimestamp'

    LEVELS = LEVEL

    ts = time.time()
    sessionall = pd.read_csv(filepath, usecols=[SESSION, EVENTTIMESTAMP, PAGESEQ, PAGELINK]).sort_values([SESSION, PAGESEQ], ascending=[1,1])

    #sessionall = DataFrame(df, columns=[SESSION, PAGESEQ, PAGELINK]).sort_values([SESSION, PAGESEQ], ascending=[1,1])

    #只保留網址中問號前的資訊
    link_split = [str(line).split(sep='?')[0] for line in sessionall[PAGELINK]]
    sessionall[PAGELINK] = link_split

    START = "start"
    EXIT = "exit"
    DL = len(sessionall) - 1

    def next_page(pool, start_page, end_page, score):
        pool.setdefault(start_page, {}).setdefault(end_page, 0)
        pool[start_page][end_page] += score

    def page_count(pool, start_page, score):
        pool.setdefault(start_page, 0)
        pool[start_page] += score

    for count, line in enumerate(sessionall.values):
        session = line[3]
        seq = line[2]
        start_page = line[0]
        #print(line)

        if start_page.find("https") == -1:
            continue

        for level in range(0, LEVELS):
            score = (LEVELS - level) / LEVELS

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
    correlationdf = DataFrame(0, index=pageall, columns=pageall)

    for start_page, exit_pages in pagedict.items():
        for exit_page, count in exit_pages.items():
            correlationdf.ix[start_page, exit_page] = count / pagecount[start_page]

    #correlationdf.to_csv(OUTPUTPATH, sep=TAB)
    return correlationdf

if __name__ == "__main__":
    Correlation("/Users/yehben/Desktop/Page_expose.txt","/Users/yehben/Desktop/output.csv",3)
