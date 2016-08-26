#!/usr/bin/python

import os

import luigi
import logging
import jaydebeapi as jdbc

logger = logging.getLogger('luigi-interface')

SEP = ","

BASEPATH = "{}/../data".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_TEMP = os.path.join(BASEPATH, "temp")
BASEPATH_RAW = os.path.join(BASEPATH, "raw")

class TeradataTable(luigi.Task):
    task_namespace = "clickstream"

    query = luigi.Parameter()
    ofile = luigi.Parameter()
    columns = luigi.Parameter()

    def run(self):
        connection = jdbc.connect('com.teradata.jdbc.TeraDriver',
                                  ['jdbc:teradata://88.8.98.214/tmode=ANSI,CLIENT_CHARSET=WINDOWS-950',
                                   'i0ac30an',
                                   'P@$$w0rd'],
                                  ['/home/rc/Documents/programs/weblogs/drivers/terajdbc4.jar',
                                   '/home/rc/Documents/programs/weblogs/drivers/tdgssconfig.jar'])
        cursor = connection.cursor()
        sql = self.query

        count_error = 0
        try:
            cursor.execute(sql)
        except jdbc.DatabaseError:
            count_error += 1

        with self.output().open('wb') as out_file:
            out_file.write("{}\n".format(",".join(self.columns.split(SEP))))

            try:
                for row in cursor.fetchall():
                    try:
                        out_file.write("{}\n".format(SEP.join([str(r) for r in row])))
                    except UnicodeEncodeError:
                        count_error += 1
            except jdbc.Error:
                pass

        # close connection
        connection.close()

        logger.log(logging.WARN, "The error count is {}".format(count_error))

    def output(self):
        return luigi.LocalTarget(self.ofile)


class RawPath(luigi.Task):
    task_namespace = "clickstream"

    date = luigi.DateParameter(default="2016-08-22")

    def requires(self):
        global BASEPATH_TEMP

        columns = "PageLocation,EventTimestamp,PageSequenceInSession,SessionNumber"
        query = "SELECT PageLocation,EventTimestamp,PageSequenceInSession,SessionNumber FROM VP_OP_ADC.page WHERE EventTimestamp >= '{date} {hour}:00:00' AND EventTimestamp <= '{date} {hour}:59:59' ORDER BY SessionNumber,PageSequenceInSession ASC"
        ofile = "{basepath}/page_{date}_{hour}.csv"

        for hour in range(0, 24):
            yield TeradataTable(query=query.format(date=self.date, hour="{:02d}".format(hour)),
                                ofile=ofile.format(basepath=BASEPATH_TEMP, date=self.date, hour="{:02d}".format(hour)),
                                columns=columns)

    def run(self):
        with self.output().open("wb") as out_file:
            out_file.write("SessionID,CookieID,CreationDatetime,nPath\n")

            pre_session_number, pre_creation_datetime, pre_sequence, pages = None, None, None, []
            for input in self.input():
                with input.open("rb") as in_file:
                    for row in in_file:
                        url, creation_datetime, sequence, session_number = row.strip().rsplit(SEP, 3)
                        if url.find("https") == -1:
                            continue

                        start_idx = url.find("?")
                        url = url[:start_idx if start_idx > -1 else len(url)]

                        if pre_session_number is not None and pre_session_number != session_number:
                            out_file.write("{},{},{},{}\n".format(pre_session_number, "cookie_id", pre_creation_datetime, ">".join(pages)))

                            pages = []

                        pages.append(url)

                        pre_session_number, pre_creation_datetime, pre_sequence = session_number, creation_datetime, sequence

            out_file.write("{},{},{},{}\n".format(pre_session_number, "cookie_id", pre_creation_datetime, ">".join(pages)))

    def output(self):
        global BASEPATH_RAW

        return luigi.LocalTarget("{}/path_{}.csv".format(BASEPATH_RAW, self.date))

class CorrelationPage(RawPath):
    task_namespace = "clickstream"

    def run(self):
        pagedict, pagecount = {}, {}

        mod = __import__("page.correlation", fromlist=[""])

        for input in self.input():
            df = mod.luigi_run(input.fn, 3, pagedict, pagecount)

        df.to_csv(self.output().fn)

    def output(self):
        global BASEPATH_RAW

        return luigi.LocalTarget("{}/two_pages_correlation_{}.csv".format(BASEPATH_RAW, self.date))


class RawSession(luigi.Task):
    task_namespace = "clickstream"

    date = luigi.DateParameter(default="2016-08-22")

    def requires(self):
        global BASEPATH_TEMP

        query = "SELECT SessionNumber,PageLoadDuration,PageViewTime,PageViewActiveTime FROM VP_OP_ADC.pagesummary WHERE EventTimestamp >= '{date} {hour}:00:00' AND EventTimestamp <= '{date} {hour}:59:59' ORDER BY SessionNumber ASC"
        ofile = "{basepath}/session_{date}_{hour}.csv"

        for hour in range(0, 24):
            yield TeradataTable(query=query.format(date=self.date, hour="{:02d}".format(hour)),
                                ofile=ofile.format(basepath=BASEPATH_TEMP, date=self.date, hour="{:02d}".format(hour)))

    def run(self):
        with self.output().open("wb") as out_file:
            out_file.write("SessionNumber,CookieID,IndividualID,PageLoadDuration,PageViewTime,LogicRatio,CountEvent,PageViewActiveTime\n")

            pre_session_number = None
            chain_length, loading_time, all_time, active_time = 0, 0, 0, 0

            for input in self.input():
                with input.open("rb") as in_file:
                    for row in in_file:
                        session_number, lt, at, att = row.strip().split(SEP)

                        if pre_session_number is not None and session_number != pre_session_number:
                            out_file.write("{},cookie_id,individual_id,{},{},logic_ratio,count_event,{}\n".format(session_number, lt, at, att))

                            chain_length, loading_time, all_time, active_time = 0, 0, 0, 0
                            
                        pre_session_number = session_number

                        chain_length += 1
                        loading_time += float(lt) if lt.isdigit() else 0.1
                        all_time += float(at) if at.isdigit() else 0.1
                        active_time += float(att) if att.isdigit() else 0.1

            out_file.write("{},cookie_id,individual_id,{},{},logic_ratio,count_event,{}\n".format(session_number, lt, at, att))

    def output(self):
        global BASEPATH_RAW

        return luigi.LocalTarget("{}/session_{}.csv".format(BASEPATH_RAW, self.date))
