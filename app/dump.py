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


class DumpAllTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()
    today = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        yield DayNADumpTask(remove=self.remove)
        yield MonthMonthDumpTask(remove=self.remove, today=self.today)
        yield YearYearDumpTask(remove=self.remove, today=self.today)
        yield MonthYearDumpTask(remove=self.remove, today=self.today)

        yield DayMonthDumpTask(remove=self.remove, today=self.today.strftime("%Y-%m-%d"))

class DayNADumpTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()

    def requires(self):
        global BASEPATH_TERADATA

        tables = ["VP_MCIF.PARTY_CC",
                  "VP_MCIF.ACCT_INS_CATHAYLIFE",
                  "VP_MCIF.ACCT_DRV_MF",
                  "VP_MCIF.ACCT_INS_PROPERTY",
                  "VP_MCIF.LOCATION_DRV",
                  "VP_MCIF.RD_MIS_MCC_CODE",
                  "VP_MCIF.RD_MIS_MCC_GROUP_CODE",
                  "VP_MCIF.RD_MIS_MERCHAINT_ID",
                  "VP_MCIF.RD_CC_CATHAY_CARD_TYPE",
                  "VP_MCIF.EVENT_CC_AIRPORT_PARKING",
                  "DP_MCIF_REF.RD_ABT_PROD_CODES"]

        for table in tables:
            sql = "SELECT * FROM {}".format(table)
            ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))

            if self.remove and os.path.exists(ofile):
                os.remove(ofile)

            yield TeradataTable(query=sql, ofile=ofile)

class DayMonthDumpTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()
    today = luigi.Parameter()

    def requires(self):
        global BASEPATH_TERADATA

        sqls = ["SELECT * FROM VP_MCIF.PARTY_DRV",
                "SELECT CUSTOMER_ID, CARD_TYPE_CATEGORY_CODE,CARD_TYPE_CODE,CARD_NBR,TXN_DATE,TXN_CODE,TXN_AMT,MERCHANT_NBR,MERCHANT_CATEGORY_CODE,MERCHANT_NAME,MERCHANT_LOCATION_CITY,MERCHANT_LOCATION_COUNTRY_CODE,ORIGINAL_CURRENCY_CODE,TXN_AMT_US_DOLLAR,PRIMARY_CARDHOLDER_IND,TREATY_CONV_AMT FROM VP_MCIF.EVENT_CC_TXN"]

        done = set()
        for diff in [0, 30, 60, 90, 120, 150, 180]:
            month = datetime.datetime.strptime(self.today, "%Y-%m-%d") - datetime.timedelta(days=diff)
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

                            if self.remove and os.path.exists(ofile):
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
    today = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        global BASEPATH_TERADATA

        year = self.today.year-1
        sqls = ["SELECT * FROM VP_MCIF.RPT_CC_COST", "SELECT * FROM VP_MCIF.RPT_CC_COST2"]

        for sql in sqls[:]:
            table = sql.split(" ")[-1]
            ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))
            sql = sql + str(year)

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

        self.today = self.today.replace(day=1)
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
