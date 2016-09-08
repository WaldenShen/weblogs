#!/usr/bin/python

import os
import luigi
import logging

from luigi import date_interval as d
from saisyo import SimpleDynamicTask, CommonPathTask, RawPageError
from page import PageCorrTask
from insert import InsertPageCorrTask

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_DB = os.path.join(BASEPATH, "data", "db")
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")


class Raw(luigi.Task):
    task_namespace = "clickstream"

    mode = luigi.Parameter(default="range")
    interval = luigi.DateIntervalParameter()

    corr = luigi.DictParameter()

    #raw_session = luigi.DictParameter()
    raw_cookie = luigi.DictParameter()

    #stats_cookie = luigi.DictParameter()
    #stats_website = luigi.DictParameter()

    def requires(self):
        global BASEPATH_DB

        if self.mode == "single":
            ofile_page_corr = os.path.join(BASEPATH_DB, "page_corr_{}.txt".format(self.interval))

            ofile_raw_session = os.path.join(BASEPATH_RAW, "session_{}.tsv.gz".format(self.interval))
            ofile_raw_cookie = os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(self.interval))

            ofile_stats_website = os.path.join(BASEPATH_RAW, "website_{}.tsv.gz".format(self.interval))
            ofile_stats_cookie = os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(self.interval))

            yield InsertPageCorrTask(interval=self.interval, ofile=ofile_page_corr, **self.corr)
            yield CommonPathTask(interval=self.interval)

            #yield SimpleDynamicTask(interval=self.interval, ofile=ofile_raw_session, **self.raw_session)
            yield SimpleDynamicTask(interval=self.interval, ofile=ofile_raw_cookie, **self.raw_cookie)

            #yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_cookie, **self.stats_website)
            #yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_website, **self.stats_cookie)

            # For Page Error
            yield RawPageError(interval=self.interval)
        elif self.mode == "range":
            for date in self.interval:
                interval = d.Date.parse(str(date))

                ofile_page_corr = os.path.join(BASEPATH_DB, "page_corr_{}.txt".format(str(date)))

                ofile_raw_session = os.path.join(BASEPATH_RAW, "session_{}.tsv.gz".format(self.interval))
                ofile_raw_cookie = os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(self.interval))

                ofile_stats_website = os.path.join(BASEPATH_RAW, "website_{}.tsv.gz".format(self.interval))
                ofile_stats_cookie = os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(self.interval))

                yield InsertPageCorrTask(interval=interval, ofile=ofile_page_corr, **self.corr)
                yield CommonPathTask(interval=interval)

                #yield SimpleDynamicTask(interval=self.interval, ofile=ofile_raw_session, **self.raw_session)
                yield SimpleDynamicTask(interval=self.interval, ofile=ofile_raw_cookie, **self.raw_cookie)

                #yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_cookie, **self.stats_website)
                #yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_website, **self.stats_cookie)

                # For Page Error
                yield RawPageError(interval=interval)
        else:
            raise NotImplementedError
