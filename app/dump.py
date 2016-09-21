#!/usr/bin/python

import os
import luigi
import logging
import datetime

from luigi import date_interval as d
from rdb import TeradataTable

logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.INFO)

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_TERADATA = os.path.join(BASEPATH, "data", "teradata")


class DayMonthDumpTask(luigi.Task):
    task_namespace = "dump"

    interval = luigi.DateIntervalParameter()

    def requires(self):
        global BASEPATH_TERADATA

        sqls = ["SELECT * FROM VP_MCIF.PARTY_DRV",
                "SELECT * FROM VP_MCIF.EVENT_CC_TXN"]

        done = set()
        for month in self.interval:
            past = month.strftime("%Y%m")

            for sql in sqls[:]:
                table = sql.split(" ")[-1]

                if past not in done:
                    if past != datetime.datetime.now().strftime("%Y%m"):
                        sql = sql + past
                        ofile = os.path.join(BASEPATH_TERADATA, "{}{}.tsv.gz".format(table, past))

                        yield TeradataTable(query=sql, ofile=ofile)
                        done.add((sql, past))
                    else:
                        if month.day == 1:
                            ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))

                            if os.path.exists(ofile):
                                os.remove(ofile)

                            yield TeradataTable(query=sql, ofile=ofile)
                            done.add((sql, past))

class MonthMonthDumpTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()
    today = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        global BASEPATH_TERADATA

        sqls = ["SELECT * FROM VP_MCIF.PARTY_DRV_VIP",
                "SELECT * FROM VP_MCIF.PARTY_VA_BUCC_PROD_STAT"]

        first = self.today.replace(day=1)
        last_month = first - datetime.timedelta(days=1)

        for sql in sqls[:]:
            table = sql.split(" ")[-1]
            ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))

            if self.remove and os.path.exists(ofile):
                os.remove(ofile)

            yield TeradataTable(query=sql+last_month.strftime("%Y%m"), ofile=ofile)

class YearYearDumpTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()
    year = luigi.IntParameter(default=datetime.datetime.now().year-1)

    def requires(self):
        global BASEPATH_TERADATA

        sqls = ["SELECT * FROM VP_MCIF.RPT_CC_COST", "SELECT * FROM VP_MCIF.RPT_CC_COST2"]

        for sql in sqls[:]:
            table = sql.split(" ")[-1]
            ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))
            sql = sql + str(self.year)

            if self.remove and os.path.exists(ofile):
                os.remove(ofile)

            yield TeradataTable(query=sql, ofile=ofile)

class MonthYearDumpTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()
    today = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        global BASEPATH_TERADATA

        sqls = ["SELECT * FROM VP_MCIF.PARTY_CC_JCIC_KRM046_DATA"]

        if self.today.day == 1:
            for sql in sqls[:]:
                table = sql.split(" ")[-1]
                ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))
                sql = sql + str(self.today.year)

                if self.remove and os.path.exists(ofile):
                    os.remove(ofile)

                yield TeradataTable(query=sql, ofile=ofile)
        else:
            logger.warn("not the first date of this month({})".format(self.today))


