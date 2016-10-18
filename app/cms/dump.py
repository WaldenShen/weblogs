#!/usr/bin/python

import os
import luigi
import logging
import datetime

from luigi import date_interval as d
from rdb import TeradataTable

logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.INFO)

BASEPATH = "{}/../..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_TERADATA = os.path.join(BASEPATH, "data", "teradata")


class DumpAllTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()
    today = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        yield DayNADumpTask(remove=self.remove)
        yield MonthMonthDumpTask(remove=self.remove, today=self.today)
        #yield YearYearDumpTask(remove=self.remove, today=self.today)
        #yield MonthYearDumpTask(remove=self.remove, today=self.today)

        yield DayMonthDumpTask(remove=self.remove, today=self.today.strftime("%Y-%m-%d"))

class DayNADumpTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()

    def requires(self):
        global BASEPATH_TERADATA

        sqls = ["SELECT CUSTOMER_ID, CUSTOMER_ID_MODIFIER, CR_EU_BANK_ACCT_IND, VIP_CODE FROM VP_MCIF.PARTY_CC",
                "SELECT MCC_CODE, MCC_GROUP_CODE FROM VP_MCIF.RD_MIS_MCC_CODE",
                "SELECT MCC_GROUP_CODE, MCC_GROUP_CODE_DESC FROM VP_MCIF.RD_MIS_MCC_GROUP_CODE",
                "SELECT MERCHANT_ID, MERCHANT_NAME, CLASS_CODE FROM DP_MCIF_REF.RD_MIS_MERCHAINT_ID",
                "SELECT AFFGROUP_CODE,CARD_TYPE_CODE,CARD_NAME,KIND_NAME,KIND1,KIND2,COMBO FROM VP_MCIF.RD_CC_CATHAY_CARD_TYPE",
                "SELECT CUSTOMER_ID, CARD_TYPE, CARD_NBR, INSIDE_DATE, OUTSIDE_DATE FROM VP_MCIF.EVENT_CC_AIRPORT_PARKING"]

        for sql in sqls:
            table = sql.split(" ")[-1]
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

        sqls = [("SELECT CUSTOMER_ID,CUSTOMER_ID_MODIFIER,CARD_NBR,DOMICILE_BRANCH,CARD_TYPE_CODE,CARDHOLDER_CUST_ID,BLOCK_CODE,CLOSED_DATE,CREDIT_LIMIT,AVAILABLE_CARD_LIMIT,ACCT_OPEN_DATE,PRIMARY_CARD_IND FROM VP_MCIF.ACCT_CC_DAILY", False),
                ("SELECT CUSTOMER_ID, GENDER_TYPE_CODE,BIRTHDAY, CUSTOMER_ROLE_CODE, CUSTOMER_CLASS_CODE FROM VP_MCIF.PARTY_DRV", False),
                ("SELECT CUSTOMER_ID,CARD_TYPE_CATEGORY_CODE,CARD_TYPE_CODE,CARD_NBR,TXN_DATE,TXN_CODE,TXN_AMT,MERCHANT_NBR,MERCHANT_CATEGORY_CODE,MERCHANT_NAME,MERCHANT_LOCATION_CITY,MERCHANT_LOCATION_COUNTRY_CODE,ORIGINAL_CURRENCY_CODE,TXN_AMT_US_DOLLAR,PRIMARY_CARDHOLDER_IND,TREATY_CONV_AMT FROM VP_MCIF.EVENT_CC_TXN", True),
                ("SELECT CUSTOMER_ID,Foreign_PB_Ind,Foreign_TD_Ind,Passbook_DP_Ind,Trust_Ind,INS_Agent_Life_IND,INS_Agent_PTY_IND,CreditCard_Ind FROM VP_MCIF.PARTY_DRV_PROD_IND", False)]

        done = set()
        for osql, dump_past in sqls[:]:
            table = osql.split(" ")[-1]

            for diff in ([0, 30, 60, 90, 120, 150, 180] if dump_past else [0]):
                month = datetime.datetime.strptime(self.today, "%Y-%m-%d") - datetime.timedelta(days=diff)
                past = month.strftime("%Y%m")

                if past not in done:
                    if past != datetime.datetime.now().strftime("%Y%m"):
                        sql = osql + past
                        ofile = os.path.join(BASEPATH_TERADATA, "{}{}.tsv.gz".format(table, past))

                        yield TeradataTable(query=sql, ofile=ofile)
                        done.add((osql, past))
                    else:
                        if month.day == 1:
                            ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))

                            if self.remove and os.path.exists(ofile):
                                os.remove(ofile)

                            yield TeradataTable(query=osql, ofile=ofile)
                            done.add((osql, past))

class MonthMonthDumpTask(luigi.Task):
    task_namespace = "dump"

    remove = luigi.BoolParameter()
    today = luigi.DateParameter(default=datetime.datetime.now())

    def requires(self):
        global BASEPATH_TERADATA

        sqls = [("SELECT CUSTOMER_ID,AVG_SAV_CHK_BAL,AVG_TIME_BAL,AVG_FUND_BAL,INSURANCE_BAL,REITS_BAL,AVG_CITA_BAL,AVG_STR_BAL,AUM_RP_BAL,AUM_TRU_BAL FROM VP_MCIF.PARTY_DRV_VIP", False),
                ("SELECT CUSTOMER_ID,TXN_DATE,TXN_TYPE,CHANGE_BONUS_POINT,AFT_AVAILABLE_BONUS_POINT,TXN_MEMO FROM VP_MCIF.EVENT_UCL_BPOINT_TXN", True)]

        first = self.today.replace(day=1)
        last_month = first - datetime.timedelta(days=1)

        for sql, dump_past in sqls[:]:
            table = sql.split(" ")[-1]
            ofile = None
            if dump_past:
                ofile = os.path.join(BASEPATH_TERADATA, "{}{}.tsv.gz".format(table, last_month.strftime("%Y%m")))
            else:
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
