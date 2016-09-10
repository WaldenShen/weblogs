#!/usr/bin/python

import os
import glob
import luigi
import logging

from luigi import date_interval as d
from saisyo import SimpleDynamicTask, CommonPathTask, RawPageError
from page import PageCorrTask
from rdb import SqlliteTable
from insert import InsertPageCorrTask

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_DB = os.path.join(BASEPATH, "data", "db")
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")
BASEPATH_ADV = os.path.join(BASEPATH, "data", "adv")
BASEPATH_STATS = os.path.join(BASEPATH, "data", "stats")


class Raw(luigi.Task):
    task_namespace = "clickstream"

    mode = luigi.Parameter(default="range")
    interval = luigi.DateIntervalParameter()

    corr = luigi.DictParameter(default={"lib": "advanced.page.correlation", "length": 4})

    raw_session = luigi.DictParameter(default={"lib": "basic.raw.session", "mode": "dict"})
    raw_cookie = luigi.DictParameter(default={"lib": "basic.raw.cookie", "mode": "dict"})

    stats_page = luigi.DictParameter(default={"lib": "basic.stats.page", "mode": "dict"})
    stats_session = luigi.DictParameter(default={"lib": "basic.stats.session", "mode": "dict"})
    stats_cookie = luigi.DictParameter(default={"lib": "basic.stats.cookie", "mode": "dict"})
    stats_website = luigi.DictParameter(default={"lib": "basic.stats.website", "mode": "dict"})

    def requires(self):
        global BASEPATH_DB, BASEPATH_RAW, BASEPATH_ADV, BASEPATH_STATS

        if self.mode == "single":
            ofile_page_corr = os.path.join(BASEPATH_DB, "pagecorrelation_{}.tsv.gz".format(self.interval))
            #yield InsertPageCorrTask(interval=self.interval, ofile=ofile_page_corr, **self.corr)

            ofile_common_path = os.path.join(BASEPATH_ADV, "common_path_{}.tsv.gz".format(self.interval))
            #yield CommonPathTask(interval=self.interval, ofile=ofile_common_path)

            ofile_raw_session = os.path.join(BASEPATH_RAW, "session_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, ofile=ofile_raw_session, **self.raw_session)

            ofile_raw_cookie = os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, ofile=ofile_raw_cookie, **self.raw_cookie)

            ofile_stats_cookie = os.path.join(BASEPATH_STATS, "cookie_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_cookie, **self.stats_cookie)

            ofile_stats_website = os.path.join(BASEPATH_STATS, "website_{}.tsv.gz".format(self.interval))
            yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_website, **self.stats_website)

            for hour in range(0, 24):
                ofile_stats_page = os.path.join(BASEPATH_STATS, "page_{}{:02d}.tsv.gz".format(self.interval, hour))
                yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_page, hour=hour, **self.stats_page)

                ofile_stats_session = os.path.join(BASEPATH_STATS, "session_{}{:02d}.tsv.gz".format(self.interval, hour))
                yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_session, hour=hour, **self.stats_session)

                ofile_stats_cookie = os.path.join(BASEPATH_STATS, "cookie_{}{:02d}.tsv.gz".format(self.interval, hour))
                yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_cookie, hour=hour, **self.stats_cookie)

                ofile_stats_website = os.path.join(BASEPATH_STATS, "website_{}{:02d}.tsv.gz".format(self.interval, hour))
                yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_website, hour=hour, **self.stats_website)

            ofile_page_error = os.path.join(BASEPATH_ADV, "pageerror_{}.tsv.gz".format(self.interval))
            #yield RawPageError(interval=self.interval, ofile=ofile_page_error)
        elif self.mode == "range":
            for date in self.interval:
                interval = d.Date.parse(str(date))

                ofile_page_corr = os.path.join(BASEPATH_DB, "pagecorrelation_{}.tsv.gz".format(str(date)))
                #yield InsertPageCorrTask(interval=interval, ofile=ofile_page_corr, **self.corr)

                ofile_common_path = os.path.join(BASEPATH_ADV, "common_path_{}.tsv.gz".format(self.interval))
                #yield CommonPathTask(interval=interval, ofile=ofile_common_path)

                ofile_raw_session = os.path.join(BASEPATH_RAW, "session_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, ofile=ofile_raw_session, **self.raw_session)

                ofile_raw_cookie = os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, ofile=ofile_raw_cookie, **self.raw_cookie)

                ofile_stats_page = os.path.join(BASEPATH_STATS, "page_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_page, **self.stats_page)

                ofile_stats_session = os.path.join(BASEPATH_STATS, "session_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=self.interval, ofile=ofile_stats_session, **self.stats_session)

                ofile_stats_cookie = os.path.join(BASEPATH_STATS, "cookie_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, ofile=ofile_stats_cookie, **self.stats_cookie)

                ofile_stats_website = os.path.join(BASEPATH_STATS, "website_{}.tsv.gz".format(str(date)))
                yield SimpleDynamicTask(interval=interval, ofile=ofile_stats_website, **self.stats_website)

                for hour in range(0, 24):
                    ofile_stats_page = os.path.join(BASEPATH_STATS, "page_{}{:02d}.tsv.gz".format(str(date), hour))
                    yield SimpleDynamicTask(interval=interval, ofile=ofile_stats_page, hour=hour, **self.stats_page)

                    ofile_stats_session = os.path.join(BASEPATH_STATS, "session_{}{:02d}.tsv.gz".format(str(date), hour))
                    yield SimpleDynamicTask(interval=interval, ofile=ofile_stats_session, hour=hour, **self.stats_session)

                    ofile_stats_cookie = os.path.join(BASEPATH_STATS, "cookie_{}{:02d}.tsv.gz".format(str(date), hour))
                    yield SimpleDynamicTask(interval=interval, ofile=ofile_stats_cookie, hour=hour, **self.stats_cookie)

                    ofile_stats_website = os.path.join(BASEPATH_STATS, "website_{}{:02d}.tsv.gz".format(str(date), hour))
                    yield SimpleDynamicTask(interval=interval, ofile=ofile_stats_website, hour=hour, **self.stats_website)

                ofile_page_error = os.path.join(BASEPATH_ADV, "pageerror_{}.tsv.gz".format(str(date)))
                #yield RawPageError(interval=interval, ofile=ofile_page_error)
        else:
            raise NotImplemented

class RDBStatsTask(luigi.Task):
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
        elif self.mode.lower() == "range":
            for stats_type in ["page", "session", "cookie", "website"]:
                for date in self.interval:
                    for hour in range(0, 24):
                        ifile = os.path.join(BASEPATH_STATS, "{}_{}{:02d}.tsv.gz".format(stats_type, str(date), hour))
                        ofile = os.path.join(BASEPATH_DB, "{}_{}{:02d}.tsv.gz".format(stats_type, str(date), hour))

                        table = "stats_{}".format(stats_type)
                        yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)

                        if hour == 0:
                            ifile = os.path.join(BASEPATH_STATS, "{}_{}.tsv.gz".format(stats_type, str(date)))
                            ofile = os.path.join(BASEPATH_DB, "{}_{}.tsv.gz".format(stats_type, str(date)))

                            table = "stats_{}".format(stats_type)
                            yield SqlliteTable(table=table, ifile=ifile, ofile=ofile)
        else:
            raise NotImplemented
