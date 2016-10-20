#!/usr/bin/python

import os
import re
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

    ofile = luigi.Parameter(default=os.path.join(BASEPATH_TERADATA, "{}.done".format(datetime.datetime.now().strftime("%Y-%m-%d"))))

    def requires(self):
        sqls = ["SELECT CUSTOMER_ID, CUSTOMER_ID_MODIFIER, CR_EU_BANK_ACCT_IND, VIP_CODE FROM VP_MCIF.PARTY_CC",
                "SELECT MCC_CODE, MCC_GROUP_CODE FROM VP_MCIF.RD_MIS_MCC_CODE",
                "SELECT MCC_GROUP_CODE, MCC_GROUP_CODE_DESC FROM VP_MCIF.RD_MIS_MCC_GROUP_CODE",
                "SELECT MERCHANT_ID, MERCHANT_NAME, CLASS_CODE FROM DP_MCIF_REF.RD_MIS_MERCHANT_ID",
                "SELECT AFFGROUP_CODE,CARD_TYPE_CODE,CARD_NAME,KIND_NAME,KIND1,KIND2,COMBO FROM VP_MCIF.RD_CC_CATHAY_CARD_TYPE",
                "SELECT CUSTOMER_ID, CARD_TYPE_CODE, CARD_NBR, INSIDE_DATE, OUTSIDE_DATE FROM VP_MCIF.EVENT_CC_AIRPORT_PARKING"]
        for sql in sqls:
            table = re.search(r"\sFROM\s([\w\d\._]+)", sql).group(1)
            ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))

            if self.remove and os.path.exists(ofile):
                os.remove(ofile)

            yield TeradataTable(query=sql, ofile=ofile)

        sqls = [("SELECT CUSTOMER_ID,CUSTOMER_ID_MODIFIER,CARD_NBR,DOMICILE_BRANCH,CARD_TYPE_CODE,CARDHOLDER_CUST_ID,BLOCK_CODE,CLOSED_DATE,CREDIT_LIMIT,AVAILABLE_CREDIT_LIMIT,ACCT_OPEN_DATE,PRIMARY_CARD_IND FROM VP_MCIF.ACCT_CC_DAILY", False),
                ("SELECT CUSTOMER_ID, GENDER_TYPE_CODE,BIRTHDAY, CUSTOMER_ROLE_CODE, CUSTOMER_CLASS_CODE FROM VP_MCIF.PARTY_DRV WHERE BIRTHDAY >= CAST('1900-01-01' AS DATE)", False),
                ("SELECT CUSTOMER_ID,CARD_TYPE_CATEGORY_CODE,CARD_TYPE_CODE,CARD_NBR,TXN_DATE,TXN_CODE,TXN_AMT,MERCHANT_NBR,MERCHANT_CATEGORY_CODE,MERCHANT_NAME,MERCHANT_LOCATION_CITY,MERCHANT_LOCATION_COUNTRY_CODE,ORIGINAL_CURRENCY_CODE,TXN_AMT_US_DOLLAR,PRIMARY_CARDHOLDER_IND,TREATY_CONV_AMT FROM VP_MCIF.EVENT_CC_TXN", True),
                ("SELECT CUSTOMER_ID,Foreign_PB_Ind,Foreign_TD_Ind,Passbook_DP_Ind,Trust_Ind,INS_Agent_Life_IND,INS_Agent_PTY_IND,CreditCard_Ind FROM VP_MCIF.PARTY_DRV_PROD_IND", False)]
        for osql, dump in sqls:
            table = re.search(r"\sFROM\s([\w\d\._]+)", osql).group(1)
            for diff in ([0, 30, 60, 90, 120, 150, 180] if dump else [0]):
                month = self.today - datetime.timedelta(days=diff)
                past = month.strftime("%Y%m")

                if past != self.today.strftime("%Y%m"):
                    sql = osql + past
                    ofile = os.path.join(BASEPATH_TERADATA, "{}{}.tsv.gz".format(table, past))

                    yield TeradataTable(query=sql, ofile=ofile)
                else:
                    ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))

                    if self.remove and os.path.exists(ofile):
                        os.remove(ofile)

                    yield TeradataTable(query=osql, ofile=ofile)

        sqls = [("SELECT CUSTOMER_ID,AVG_SAV_CHK_BAL,AVG_TIME_BAL,AVG_FUND_BAL,INSURANCE_BAL,REITS_BAL,AVG_CITA_BAL,AVG_STR_BAL,AUM_RP_BAL,AUM_TRU_BAL FROM VP_MCIF.PARTY_DRV_VIP", False),
                ("SELECT CUSTOMER_ID,TXN_DATE,TXN_TYPE,CHANGE_BONUS_POINT,AFT_AVAILABLE_BONUS_POINT,TXN_MEMO FROM VP_MCIF.EVENT_UCL_BPOINT_TXN", True)]
        for sql, dump in sqls:
            first = self.today.replace(day=1)
            last_month = first - datetime.timedelta(days=1)

            ofile = None
            table = re.search(r"\sFROM\s([\w\d\._]+)", sql).group(1)
            if dump:
                ofile = os.path.join(BASEPATH_TERADATA, "{}{}.tsv.gz".format(table, last_month.strftime("%Y%m")))
            else:
                ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))

            yield TeradataTable(query=sql+last_month.strftime("%Y%m"), ofile=ofile)

        '''
        sqls = ["SELECT * FROM VP_MCIF.RPT_CC_COST", "SELECT * FROM VP_MCIF.RPT_CC_COST2"]
        for osql in sqls:
            year = self.today.year-1

            table = re.search(r"\sFROM\s([\w\d\._]+)", osql).group(1)
            ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))
            sql = osql + str(year)

            if self.remove and os.path.exists(ofile):
                os.remove(ofile)

            yield TeradataTable(query=sql, ofile=ofile)

        sqls = ["SELECT * FROM VP_MCIF.PARTY_CC_JCIC_KRM046_DATA"]
        for osql in sqls:
            today = self.today.replace(day=1)
            if today.day == 1:
                table = re.search(r"\sFROM\s([\w\d\._]+)", osql).group(1)
                ofile = os.path.join(BASEPATH_TERADATA, "{}.tsv.gz".format(table))
                sql = osql + str(self.today.year)

                if self.remove and os.path.exists(ofile):
                    os.remove(ofile)

            yield TeradataTable(query=sql, ofile=ofile)
        '''

    def run(self):
        with self.output().open("wb") as out_file:
            pass

    def output(self):
        return luigi.LocalTarget(self.ofile)
