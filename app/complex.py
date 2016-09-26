#!/usr/bin/python
# coding=UTF-8

import os
import re
import gzip
import json
import redis
import luigi
import logging
from datetime import datetime

from saisyo import RawPath
from advanced.page import suffix_tree

from utils import get_date_type, parse_datetime, parse_raw_page, is_app_log, norm_str
from utils import load_behavior, save_behavior, load_cookie_history, save_cookie_history

from utils import SEP, NEXT, ENCODE_UTF8, UNKNOWN, INTERVAL
from utils import ALL_CATEGORIES, LOGIC1, LOGIC2, FUNCTION, INTENTION

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")
BASEPATH_STATS = os.path.join(BASEPATH, "data", "stats")


class CommonPathTask(luigi.Task):
    task_namespace = "clickstream"

    ofile = luigi.Parameter()

    ntype = luigi.Parameter()
    interval = luigi.DateIntervalParameter()

    def requires(self):
        global BASEPATH_RAW

        ofile = "{}/{}path_{}.tsv.gz".format(BASEPATH_RAW, self.ntype, self.interval)
        yield RawPath(ntype=self.ntype.replace("_", ""), interval=self.interval, ofile=ofile)

    def run(self):
        common_path = suffix_tree.CommonPath()

        for input in self.input():
            with input.open("rb") as in_file:
                is_header = True

                for line in in_file:
                    if is_header:
                        is_header = False
                    else:
                        session_id, _, _, path = line.decode(ENCODE_UTF8).strip().split(SEP)
                        common_path.plant_tree(session_id, path.split(NEXT))

        creation_datetime, date_type = get_date_type(self.output().fn)
        with self.output().open("wb") as out_file:
            for j in common_path.print_tree(creation_datetime, date_type):
                try:
                    out_file.write(bytes("{}\n".format(j), ENCODE_UTF8))
                except:
                    out_file.write("{}\n".format(j))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class PageCorrTask(RawPath):
    task_namespace = "clickstream"
    priority = 20

    ofile = luigi.Parameter()

    lib = luigi.Parameter()
    length = luigi.IntParameter(default=2)

    def run(self):
        pagedict, pagecount = {}, {}

        mod = __import__(self.lib, fromlist=[""])

        df = None
        for input in self.input():
            logger.info("Start to process {}({}, {})".format(input.fn, len(pagedict), len(pagecount)))
            df = mod.luigi_run(input.fn, self.ntype, self.length, pagedict, pagecount)

        with self.output().open("wb") as out_file:
            creation_datetime, date_type = get_date_type(self.output().fn)

            for d in mod.get_json(df, self.ntype, date_type, str(self.interval), self.length):
                try:
                    out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
                except:
                    out_file.write("{}\n".format(json.dumps(d)))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class RetentionTask(luigi.Task):
    task_namespace = "clickstream"

    lib = luigi.Parameter()

    date = luigi.DateParameter()
    ofile = luigi.Parameter()

    def run(self):
        mod = __import__(self.lib, fromlist=[""])

        df = mod.luigi_run(datetime.combine(self.date, datetime.min.time()), {})
        with self.output().open("wb") as out_file:
            creation_datetime, date_type = get_date_type(self.output().fn)

            mod.luigi_dump(out_file, df, creation_datetime, date_type)

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class NALTask(luigi.Task):
    task_namespace = "clickstream"

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    def run(self):
        creation_datetime, date_type = get_date_type(self.output().fn)

        results = {"creation_datetime": creation_datetime, "count_new_pv": 0, "count_old_pv": 0, "count_new_uv": 0, "count_old_uv": 0}

        with gzip.open(self.ifile) as in_file:
            for line in in_file:
                o = json.loads(line.decode(ENCODE_UTF8).strip())

                cookie_id = o["cookie_id"]
                creation_datetime = o["creation_datetime"]
                if creation_datetime.find(".") > -1:
                    creation_datetime = datetime.strptime(creation_datetime, "%Y-%m-%d %H:%M:%S.%f")
                else:
                    creation_datetime = datetime.strptime(creation_datetime, "%Y-%m-%d %H:%M:%S")

                history = load_cookie_history(cookie_id)
                if history:
                    first_datetime = datetime.strptime(history[0], "%Y-%m-%d %H:%M:%S")

                    if creation_datetime.strftime("%Y%m%d") > first_datetime.strftime("%Y%m%d"):
                        results["count_old_pv"] += sum([c for c in o[FUNCTION].values()])
                        results["count_old_uv"] += 1
                    else:
                        results["count_new_pv"] += sum([c for c in o[FUNCTION].values()])
                        results["count_new_uv"] += 1
                else:
                    logger.warn("Not found {}".format(cookie_id))

                    results["count_new_pv"] += sum([c for c in o[FUNCTION].values()])
                    results["count_new_uv"] += 1

                    save_cookie_history(cookie_id, creation_datetime)

        with self.output().open("wb") as out_file:
            out_file.write(json.dumps(results))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class TableauPageTask(luigi.Task):
    task_namespace = "clickstream"

    ifiles = luigi.ListParameter()
    ofile = luigi.Parameter()

    def run(self):
        global ENCODE_UTF8

        with self.output().open("wb") as out_file:
            out_file.write("{}\n".format(SEP.join(["cookie_id", "individual_id", "creation_datetime", "product1", u"product2", "function", "intention", "product" , "product1_function", "product2_function", "product1_intention", "product2_intention"])))

            for filepath in self.ifiles:
                with gzip.open(filepath, "rb") as in_file:
                    is_header = True
                    for line in in_file:
                        if is_header:
                            is_header = False
                        else:
                            session_id, cookie_id, individual_id, url, creation_datetime,\
                            logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention,\
                            duration, active_duration, loading_duration = parse_raw_page(line)

                            if is_app_log(url):
                                continue

                            terms = [cookie_id, individual_id, creation_datetime, logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention]
                            for idx, term in enumerate(terms):
                                try:
                                    out_file.write(norm_str(term))
                                except UnicodeEncodeError as e:
                                    out_file.write("UnicodeEncodeError")

                                if idx < len(terms)-1:
                                    out_file.write(SEP)

                            out_file.write("{}\n".format(SEP.join([str(duration), str(active_duration), str(loading_duration)])))

                logger.info("finish {}".format(filepath))


    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class CookieHistoryTask(luigi.Task):
    task_namespace = "clickstream"

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    def run(self):
        global ENCODE_UTF8, UNKNOWN, ALL_CATEGORIES

        with self.output().open("wb") as out_file:
            with gzip.open(self.ifile, "rb") as in_file:
                for line in in_file:
                    o = json.loads(line.decode(ENCODE_UTF8))
                    profile_id, cookie_id, creation_datetime = o["individual_id"], o["cookie_id"], parse_datetime(o["creation_datetime"])
                    creation_datetime = creation_datetime.replace(microsecond=0)

                    history = load_cookie_history(cookie_id)
                    if history:
                        first_datetime, pre_datetime = None, None
                        for idx, login_datetime in enumerate([datetime.strptime(d, "%Y-%m-%d %H:%M:%S") for d in history]):
                            if first_datetime is None:
                                first_datetime = login_datetime

                            diff_seconds = 0
                            if pre_datetime is not None:
                                diff_seconds = (login_datetime - pre_datetime).total_seconds()

                            if creation_datetime == login_datetime:
                                key = "TIME_{}".format(idx+1)

                                for subkey in ALL_CATEGORIES:
                                    values = o[subkey]

                                    tc, total_count = 0, 0
                                    for name, value in values.items():
                                        if UNKNOWN not in name and name.find(u"其他") == -1:
                                            total_count += value

                                        tc += value

                                    for name, value in values.items():
                                        name = name.replace(" ", "").replace(u"投資理財", u"理財投資")

                                        results = {"individual_id": profile_id,
                                                   "cookie_id": cookie_id,
                                                   "category_type": subkey,
                                                   "times": idx+1,
                                                   "creation_datetime": login_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                                                   "first_interval": (login_datetime-first_datetime).total_seconds(),
                                                   "prev_interval": diff_seconds,
                                                   "category_key": norm_str(name),
                                                   "category_value": value,
                                                   "total_count1": tc,
                                                   "total_count2": total_count}

                                        out_file.write("{}\n".format(json.dumps(results)))
                                break

                            pre_datetime = login_datetime
                    else:
                        logger.warn("Not found {} in 'login' database in {}".format(cookie_id, self.ifile))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class IntervalTask(luigi.Task):
    task_namespace = "clickstream"

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    def requires(self):
        global BASEPATH_STATS
        creation_datetime, _ = get_date_type(self.ofile)
        ofile = os.path.join(BASEPATH_STATS, "cookiehistory_{}.tsv.gz".format(creation_datetime))

        yield CookieHistoryTask(ifile=self.ifile, ofile=ofile)

    def run(self):
        global ENCODE_UTF8, LOGIC1, LOGIC2, FUNCTION, INTENTION, ALL_CATEGORIES, INTERVAL

        results = {}
        for input in self.input():
            with input.open("rb") as in_file:
                for line in in_file:
                    o = json.loads(line.decode(ENCODE_UTF8).strip())

                    cookie_id, total_count2 = o["cookie_id"], o["total_count2"]
                    category_type, category_key, category_value = o["category_type"], o["category_key"], o["category_value"]
                    prev_interval = o["prev_interval"]

                    if total_count2 > 0 and prev_interval > 0:
                        results.setdefault(cookie_id, {}).setdefault(category_type, {}).setdefault(category_key, category_value)
                        results[cookie_id][INTERVAL] = [0, 0]
                        results[cookie_id][INTERVAL][0] += prev_interval
                        results[cookie_id][INTERVAL][1] += 1

                        #logger.info((cookie_id, category_type, category_key, category_value, results[cookie_id][category_type][category_key], results[cookie_id][INTERVAL]))

        with self.output().open("wb") as out_file:
            for cookie_id, record in results.items():
                save_behavior(cookie_id, record)

                record["cookie_id"] = cookie_id
                out_file.write("{}\n".format(json.dumps(record)))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class MappingTask(luigi.Task):
    task_namespace = "clickstream"

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    def run(self):
        global ENCODE_UTF8

        with self.output().open("wb") as out_file:
            with gzip.open(self.ifile, "rb") as in_file:
                for line in in_file:
                    o = json.loads(line.decode(ENCODE_UTF8))
                    cookie_id, profile_id, creation_datetime = o["cookie_id"], o["individual_id"], o["creation_datetime"]

                    out_file.write("{}\n".format(json.dumps({"cookie_id": cookie_id, "individual_id": profile_id, "creation_datetime": creation_datetime})))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)
