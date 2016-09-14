#!/usr/bin/python

import os
import json
import datetime

import pandas as pd
import luigi
import logging
import jaydebeapi as jdbc

from luigi import date_interval as d

from rdb import TeradataTable
from utils import load_category, norm_url, get_date_type
from utils import SEP, NEXT, ENCODE_UTF8

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_TEMP = os.path.join(BASEPATH, "data", "temp")
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")
BASEPATH_ADV = os.path.join(BASEPATH, "data", "adv")
BASEPATH_DRIVER = os.path.join(BASEPATH, "drivers")
FILEPATH_CATEGORY = os.path.join(BASEPATH, "data", "setting", "category.tsv")


class ClickstreamFirstRaw(luigi.Task):
    task_namespace = "clickstream"

    date = luigi.DateParameter()
    hour = luigi.IntParameter()

    ofile = luigi.Parameter()
    columns = luigi.Parameter(default="session_id,cookie_id,individual_id,session_seq,url,creation_datetime,function,logic,intention,duration,active_duration,loading_time,ip")

    def run(self):
        global BASEPATH_DRIVER, FILEPATH_CATEGORY

        table = ""
        if self.date.month != datetime.datetime.now().month:
            table = "{}{:02d}".format(self.date.year, self.date.month)

        category = load_category(FILEPATH_CATEGORY)

        results = {}

        connection = jdbc.connect('com.teradata.jdbc.TeraDriver',
                                  ['jdbc:teradata://88.8.98.214/tmode=ANSI,CLIENT_CHARSET=WINDOWS-950',
                                   'i0ac30an',
                                   'P@$$w0rd'],
                                  ['{}/terajdbc4.jar'.format(BASEPATH_DRIVER),
                                   '{}/tdgssconfig.jar'.format(BASEPATH_DRIVER)])
        cursor = connection.cursor()

        sql_1 = "SELECT A.sessionnumber, A.pagesequenceinsession, A.pagelocation, A.eventtimestamp, B.PageViewTime, B.PageViewActiveTime, COALESCE(B.PageLoadDuration,-1) FROM VP_OP_ADC.page{table} A LEFT JOIN VP_OP_ADC.pagesummary{table} B ON A.sessionnumber = B.sessionnumber AND A.pageinstanceid = B.pageinstanceid WHERE A.eventtimestamp >= '{date} {hour}:00:00' AND A.eventtimestamp < '{date} {hour}:59:59' ORDER BY A.sessionnumber, A.pagesequenceinsession".format(table=table, date=self.date, hour="{:02d}".format(self.hour))
        logger.info(sql_1)

        cursor.execute(sql_1)
        for row in cursor.fetchall():
            try:
                session_number, seq, url, creation_datetime, duration, active_duration, loading_duration = row
                url = url.lower()

                function, logic, intention = None, None, None
                if url in category:
                    function, logic, intention = category[url]["function"], category[url]["logic"], category[url]["intention"]

                results.setdefault(session_number, [])
                results[session_number].append(["cookie_id", "individual_id", seq, url, creation_datetime, function, logic, intention, duration, active_duration, loading_duration, "ip"])
            except UnicodeEncodeError as e:
                logger.warn(e)

        sql_2 = "SELECT sessionnumber, MAX(CookieUniqueVisitorTrackingId) FROM VP_OP_ADC.visitor{table} WHERE eventtimestamp >= '{date} {hour}:00:00' AND eventtimestamp < '{date} {hour}:59:59' GROUP BY sessionnumber".format(table=table, date=self.date, hour="{:02d}".format(self.hour))
        logger.info(sql_2)

        cursor.execute(sql_2)
        for row in cursor.fetchall():
            try:
                session_number, cookie_id = row
                if session_number in results:
                    for idx in range(0, len(results[session_number])):
                        results[session_number][idx][0] = cookie_id
                else:
                    logger.warn("The cookie_id({}) does NOT exist in results based on session_id({})".format(cookie_id, session_number))
            except UnicodeEncodeError as e:
                logger.warn(e)

        sql_3 = "SELECT sessionnumber, MAX(ProfileUiid) FROM VP_OP_ADC.individual{table} WHERE eventtimestamp >= '{date} {hour}:00:00' AND eventtimestamp < '{date} {hour}:59:59' GROUP BY sessionnumber".format(table=table, date=self.date, hour="{:02d}".format(self.hour))
        logger.info(sql_3)

        cursor.execute(sql_3)
        for row in cursor.fetchall():
            try:
                session_number, profile_id = row
                if session_number in results:
                    for idx in range(0, len(results[session_number])):
                        results[session_number][idx][1] = profile_id
                else:
                    logger.warn("The profile_id({}) does NOT exist in results based on session_id({})".format(profile_id, session_number))
            except UnicodeEncodeError as e:
                logger.warn(e)

        sql_4 = "SELECT sessionnumber, DeviceIPAddress FROM VP_OP_ADC.sessionstart{table} WHERE eventtimestamp >= '{date} {hour}:00:00' AND eventtimestamp < '{date} {hour}:59:59'".format(table=table, date=self.date, hour="{:02d}".format(self.hour))
        logger.info(sql_4)

        cursor.execute(sql_4)
        for row in cursor.fetchall():
            try:
                session_number, ip = row
                if session_number in results:
                    for idx in range(0, len(results[session_number])):
                        results[session_number][idx][-1] = ip
                else:
                    logger.warn("The ip({}) does NOT exist in results based on session_id({})".format(ip, session_number))
            except UnicodeEncodeError as e:
                logger.warn(e)

        with self.output().open('wb') as out_file:
            out_file.write(bytes("{}\n".format(SEP.join(self.columns.split(","))), ENCODE_UTF8))

            for session_id, info in results.items():
                for row in info:
                    out_file.write(bytes("{}\n".format(SEP.join(str(r) for r in [session_id] + row)), ENCODE_UTF8))

        # close connection
        connection.close()

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)


class RawPath(luigi.Task):
    task_namespace = "clickstream"

    columns = luigi.Parameter(default="session_id,cookie_id,individual_id,session_seq,url,creation_datetime,function,logic,intention,duration,active_duration,loading_time,ip")

    interval = luigi.DateIntervalParameter()
    hour = luigi.IntParameter(default=-1)

    def requires(self):
        global BASEPATH_TEMP

        ofile = "{basepath}/page_{date}_{hour}.csv.gz"
        if self.hour == -1:
            for date in self.interval:
                for hour in range(0, 24):
                    yield ClickstreamFirstRaw(date=date, hour=hour,
                                              ofile=ofile.format(basepath=BASEPATH_TEMP, date=date, hour="{:02d}".format(hour)),
                                              columns=self.columns)
        else:
            yield ClickstreamFirstRaw(date=datetime.datetime.strptime(str(self.interval), "%Y-%m-%d"), hour=self.hour,
                                      ofile=ofile.format(basepath=BASEPATH_TEMP, date=str(self.interval), hour="{:02d}".format(self.hour)),
                                      columns=self.columns)

    def run(self):
        with self.output().open("wb") as out_file:
            out_file.write(bytes(SEP.join(["session_id", "cookie_id", "creation_datetime", "npath\n"]), ENCODE_UTF8))
            #out_file.write(SEP.join(["session_id", "cookie_id", "creation_datetime", "npath\n"]))

            pre_session_number, pre_creation_datetime, pre_sequence, pages = None, None, None, []
            for input in self.input():
                with input.open("r") as in_file:
                    for row in in_file:
                        # 0: session_id
                        # 1: cookie_id
                        # 2: individual_id
                        # 3: session_seq
                        # 4: url
                        # 5: creation_datetime
                        # 6: function
                        # 7: logic
                        # 8: Intention
                        # 9: duration
                        # 10: active_duration
                        # 11: loading_time
                        # 12: ip

                        info = row.decode(ENCODE_UTF8).strip().split(SEP)
                        session_number, _, _, sequence, url, creation_datetime, _, _, _, _, _, _, _ = info
                        if url.find("https") == -1:
                            continue

                        url = norm_url(url)

                        if pre_session_number is not None and pre_session_number != session_number:
                            out_file.write(bytes("{}{sep}{}{sep}{}{sep}{}\n".format(pre_session_number, "cookie_id", pre_creation_datetime, NEXT.join(pages), sep=SEP), ENCODE_UTF8))

                            pages = []

                        pages.append(url)

                        pre_session_number, pre_creation_datetime, pre_sequence = session_number, creation_datetime, sequence

            out_file.write(bytes("{}{sep}{}{sep}{}{sep}{}\n".format(pre_session_number, "cookie_id", pre_creation_datetime, NEXT.join(pages), sep=SEP), ENCODE_UTF8))

    def output(self):
        global BASEPATH_RAW

        return luigi.LocalTarget("{}/path_{}.csv.gz".format(BASEPATH_RAW, self.interval), format=luigi.format.Gzip)

class RawPageError(luigi.Task):
    task_namespace = "clickstream"

    ofile = luigi.Parameter()
    interval = luigi.DateIntervalParameter()

    def requires(self):
        global BASEPATH_TEMP

        columns = "SessionNumber,PageInstanceID,EventTimestamp,ErrorDescription"
        query = "SELECT SessionNumber,PageInstanceID,EventTimestamp,ErrorDescription FROM VP_OP_ADC.pageerror{table} WHERE eventtimestamp >= '{date} {hour}:00:00' AND eventtimestamp < '{date} {hour}:59:59'"
        ofile = "{}/page_error_{}_{}.csv.gz"

        for date in self.interval:
            for hour in range(0, 24):
                table = ""
                if date.month != datetime.datetime.now().month:
                    table = "{}{:02d}".format(date.year, date.month)

                yield TeradataTable(columns=columns,
                                    query=query.format(table=table, date=date, hour="{:02d}".format(hour)),
                                    ofile=ofile.format(BASEPATH_TEMP, date, "{:02d}".format(hour)))

    def run(self):
        count = 0

        for input in self.input():
            logger.info("Start to process {}".format(input.fn))

            with input.open("rb") as in_file:
                for line in in_file:
                    count += 1

        with self.output().open("wb") as out_file:
            out_file.write(bytes("{}\n".format(count), ENCODE_UTF8))
            #out_file.write("{}\n".format(count))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class SimpleDynamicTask(RawPath):
    task_namespace = "clickstream"

    ofile = luigi.Parameter()

    lib = luigi.Parameter()
    mode = luigi.Parameter()

    def run(self):
        pagedict, pagecount = {}, {}

        mod = __import__(self.lib, fromlist=[""])

        if self.mode.lower() == "dict":
            df = {}
            is_first = True

            for input in self.input():
                logger.info("Start to process {}".format(input.fn))
                df = mod.luigi_run(input.fn, is_first, df)

                is_first = False

            with self.output().open("wb") as out_file:
                creation_datetime, date_type = get_date_type(self.output().fn)

                mod.luigi_dump(out_file, df, creation_datetime, date_type)
        elif self.mode.lower() == "list":
            with self.output().open("wb") as out_file:
                for input in self.input():
                    logger.info("Start to process {}".format(input.fn))

                    creation_datetime, date_type = get_date_type(self.output().fn)
                    for row in mod.luigi_run(input.fn):
                        out_file.write(bytes("{}\n".format(json.dumps(row)), ENCODE_UTF8))
        else:
            raise NotImplemented

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)
