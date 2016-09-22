#!/usr/bin/python

import os
import glob
import luigi
import logging
import datetime

from luigi import date_interval as d
from saisyo import SimpleDynamicTask, RawPageError
from complex import PageCorrTask, RetentionTask, CommonPathTask
from rdb import SqlliteTable
from insert import InsertPageCorrTask

logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.INFO)

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_DB = os.path.join(BASEPATH, "data", "db")
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")
BASEPATH_ADV = os.path.join(BASEPATH, "data", "adv")
BASEPATH_STATS = os.path.join(BASEPATH, "data", "stats")


class RawTask(luigi.Task):
    task_namespace = "clickstream"

    mode = luigi.Parameter(default="range")
    interval = luigi.DateIntervalParameter()

    raw_session = luigi.DictParameter(default={"lib": "basic.raw.session", "mode": "dict"})
    raw_cookie = luigi.DictParameter(default={"lib": "basic.raw.cookie", "mode": "dict"})

    stats_page = luigi.DictParameter(default={"lib": "basic.stats.page", "mode": "dict"})
    stats_session = luigi.DictParameter(default={"lib": "basic.stats.session", "mode": "dict"})
    stats_cookie = luigi.DictParameter(default={"lib": "basic.stats.cookie", "mode": "dict"})
    stats_website = luigi.DictParameter(default={"lib": "basic.stats.website", "mode": "dict"})

    def requires(self):
        global BASEPATH_RAW, BASEPATH_STATS

        if self.mode == "single":
            ofile_raw_session = os.path.join(BASEPATH_RAW, "session_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, filter_app=True, ofile=ofile_raw_session, **self.raw_session)

            ofile_raw_cookie = os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, filter_app=True, ofile=ofile_raw_cookie, **self.raw_cookie)

            ofile_stats_page = os.path.join(BASEPATH_STATS, "page_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, filter_app=True, ofile=ofile_stats_page, **self.stats_page)

            ofile_stats_session = os.path.join(BASEPATH_STATS, "session_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, filter_app=True, ofile=ofile_stats_session, **self.stats_session)

            ofile_stats_cookie = os.path.join(BASEPATH_STATS, "cookie_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, filter_app=True, ofile=ofile_stats_cookie, **self.stats_cookie)

            ofile_stats_website = os.path.join(BASEPATH_STATS, "website_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, filter_app=True, ofile=ofile_stats_website, **self.stats_website)

            ofile_page_error = os.path.join(BASEPATH_RAW, "pageerror_{}.tsv.gz".format(self.interval))
            yield RawPageError(interval=self.interval, ofile=ofile_page_error)
        elif self.mode == "range":
            for date in self.interval:
                interval = d.Date.parse(str(date))

                ofile_raw_session = os.path.join(BASEPATH_RAW, "session_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_raw_session, **self.raw_session)

                ofile_raw_cookie = os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_raw_cookie, **self.raw_cookie)

                ofile_stats_page = os.path.join(BASEPATH_STATS, "page_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_stats_page, **self.stats_page)

                ofile_stats_session = os.path.join(BASEPATH_STATS, "session_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_stats_session, **self.stats_session)

                ofile_stats_cookie = os.path.join(BASEPATH_STATS, "cookie_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_stats_cookie, **self.stats_cookie)

                ofile_stats_website = os.path.join(BASEPATH_STATS, "website_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_stats_website, **self.stats_website)

                ofile_page_error = os.path.join(BASEPATH_RAW, "pageerror_{}.tsv.gz".format(str(date)))
                yield RawPageError(interval=interval, ofile=ofile_page_error)

                for hour in range(0, 24):
                    ofile_stats_page = os.path.join(BASEPATH_STATS, "page_{}{:02d}.tsv.gz".format(str(date), hour))
                    yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_stats_page, hour=hour, **self.stats_page)

                    ofile_stats_session = os.path.join(BASEPATH_STATS, "session_{}{:02d}.tsv.gz".format(str(date), hour))
                    yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_stats_session, hour=hour, **self.stats_session)

                    ofile_stats_cookie = os.path.join(BASEPATH_STATS, "cookie_{}{:02d}.tsv.gz".format(str(date), hour))
                    yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_stats_cookie, hour=hour, **self.stats_cookie)

                    ofile_stats_website = os.path.join(BASEPATH_STATS, "website_{}{:02d}.tsv.gz".format(str(date), hour))
                    yield SimpleDynamicTask(interval=interval, filter_app=True, ofile=ofile_stats_website, hour=hour, **self.stats_website)
        else:
            raise NotImplementedError

class AdvancedTask(luigi.Task):
    task_namespace = "clickstream"

    mode = luigi.Parameter(default="range")
    trackday = luigi.IntParameter(default=56)
    interval = luigi.DateIntervalParameter()

    adv_corr = luigi.DictParameter(default={"lib": "advanced.page.correlation", "length": 4})
    adv_retention = luigi.DictParameter(default={"lib": "advanced.cookie.retention"})

    def requires(self):
        global BASEPATH_RAW, BASEPATH_ADV

        if self.mode.lower() == "single":
            for node_type in ["logic1", "logic2", "function", "intention"]:
                ofile_page_corr = os.path.join(BASEPATH_ADV, "{}corr_{}.tsv.gz".format(node_type, self.interval))
                yield PageCorrTask(ofile=ofile_page_corr, interval=self.interval, ntype=node_type, **self.adv_corr)

            for node_type in ["logic1", "logic2", "function", "intention", "logic", "logic1_function", "logic2_function", "logic1_intention", "logic2_intention"]:
                ofile_common_path = os.path.join(BASEPATH_ADV, "{}commonpath_{}.tsv.gz".format(node_type.replace("_", ""), self.interval))
                yield CommonPathTask(ntype=node_type, interval=self.interval, ofile=ofile_common_path)
        elif self.mode.lower() == "range":
            for date in self.interval:
                interval = d.Date.parse(str(date))

                # 4 weeks data
                ifiles = []
                now = datetime.datetime.strptime(str(date), "%Y-%m-%d")
                ofile_retention_path = os.path.join(BASEPATH_ADV, "retention_{}.tsv.gz".format(str(date)))
                yield RetentionTask(date=(now-datetime.timedelta(days=self.trackday)), ofile=ofile_retention_path, **self.adv_retention)

                '''
                for node_type in ["url", "logic1", "logic2", "function", "intention"]:
                    ofile_page_corr = os.path.join(BASEPATH_ADV, "{}corr_{}.tsv.gz".format(node_type, str(date)))
                    yield PageCorrTask(ofile=ofile_page_corr, interval=interval, ntype=node_type, **self.adv_corr)

                for hour in range(0, 24):
                    for node_type in ["url", "logic1", "logic2", "function", "intention"]:
                        ofile_page_corr = os.path.join(BASEPATH_ADV, "{}corr_{}{:02d}.tsv.gz".format(node_type, str(date), hour))
                        yield PageCorrTask(ofile=ofile_page_corr, interval=interval, hour=hour, ntype=node_type, **self.adv_corr)
                '''
        else:
            raise NotImplementedError

class RDBTask(luigi.Task):
    task_namespace = "clickstream"

    mode = luigi.Parameter(default="range")
    interval = luigi.DateIntervalParameter(default="2016-09-01-2016-09-03")

    def requires(self):
        global BASEPATH_STATS

        if self.mode.lower() == "single":
            for stats_type in ["page", "session", "cookie", "website"]:
                ifile = os.path.join(BASEPATH_STATS, "{}_{}.tsv.gz".format(stats_type, self.interval))
                ofile = os.path.join(BASEPATH_DB, "{}_{}.tsv.gz".format(stats_type, self.interval))

                table = "stats_{}".format(stats_type)
                yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)

            for node_type in ["logic1", "logic2", "function", "intention"]:
                ifile = os.path.join(BASEPATH_ADV, "{}corr_{}.tsv.gz".format(node_type, self.interval))
                ofile = os.path.join(BASEPATH_DB, "{}corr_{}.tsv.gz".format(node_type, self.interval))

                table = "adv_pagecorr"
                yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)
        elif self.mode.lower() == "range":
            for stats_type in ["page", "session", "cookie", "website"]:
                for date in self.interval:
                    for hour in range(0, 24):
                        ifile = os.path.join(BASEPATH_STATS, "{}_{}{:02d}.tsv.gz".format(stats_type, str(date), hour))
                        ofile = os.path.join(BASEPATH_DB, "{}_{}{:02d}.tsv.gz".format(stats_type, str(date), hour))

                        table = "stats_{}".format(stats_type)
                        yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)

                    ifile = os.path.join(BASEPATH_STATS, "{}_{}.tsv.gz".format(stats_type, str(date)))
                    ofile = os.path.join(BASEPATH_DB, "{}_{}.tsv.gz".format(stats_type, str(date)))

                    table = "stats_{}".format(stats_type)
                    yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)

            '''
            table = "adv_pagecorr"
            for node_type in ["url", "logic", "function", "intention"]:
                for date in self.interval:
                    for hour in range(0, 24):
                        ifile = os.path.join(BASEPATH_ADV, "{}corr_{}{:02d}.tsv.gz".format(node_type, str(date), hour))
                        ofile = os.path.join(BASEPATH_DB, "{}corr_{}{:02d}.tsv.gz".format(node_type, str(date), hour))

                        yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)

                    ifile = os.path.join(BASEPATH_ADV, "{}corr_{}.tsv.gz".format(node_type, str(date)))
                    ofile = os.path.join(BASEPATH_DB, "{}corr_{}.tsv.gz".format(node_type, str(date)))

                    yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)
            '''

            table = "adv_retention"
            for date in self.interval:
                ifile = os.path.join(BASEPATH_ADV, "retention_{}.tsv.gz".format(str(date)))
                ofile = os.path.join(BASEPATH_DB, "retention_{}.tsv.gz".format(str(date)))

                if os.path.exists(ifile):
                    yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)
        else:
            raise NotImplementedError
