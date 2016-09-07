#!/usr/bin/python

import os
import luigi
import logging

from luigi import date_interval as d
from saisyo import DynamicTask, CommonPathTask, RawPageError
from insert import InsertPageCorrTask

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_DB = os.path.join(BASEPATH, "data", "db")


class Raw(luigi.Task):
    task_namespace = "clickstream"

    mode = luigi.Parameter(default="range")
    interval = luigi.DateIntervalParameter()

    corr = luigi.DictParameter()

    def requires(self):
        global BASEPATH_DB

        if self.mode == "single":
            ofile_page_corr = os.path.join(BASEPATH_DB, "page_corr_{}.txt".format(self.interval))

            yield InsertPageCorrTask(interval=self.interval, ofile=ofile_page_corr, **self.corr)
            yield CommonPathTask(interval=self.interval)

            # For Page Error
            yield RawPageError(interval=self.interval)
        elif self.mode == "range":
            for date in self.interval:
                interval = d.Date.parse(str(date))
                ofile_page_corr = os.path.join(BASEPATH_DB, "page_corr_{}.txt".format(str(date)))

                yield InsertPageCorrTask(interval=interval, ofile=ofile_page_corr, **self.corr)
                yield CommonPathTask(interval=interval)

                # For Page Error
                yield RawPageError(interval=interval)
        else:
            raise NotImplementedError
