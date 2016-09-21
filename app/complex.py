#!/usr/bin/python

import os
import json
import luigi
import logging

from saisyo import RawPath
from advanced.page import suffix_tree
from utils import get_date_type, SEP, NEXT, ENCODE_UTF8

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")


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

    ifile = luigi.ListParameter()
    ofile = luigi.Parameter()

    def run(self):
        mod = __import__(self.lib, fromlist=[""])

        df = {}
        for fn in self.ifile:
            logger.info("Start to process {}".format(fn))
            df = mod.luigi_run(fn, df)

        with self.output().open("wb") as out_file:
            creation_datetime, date_type = get_date_type(self.output().fn)

            mod.luigi_dump(out_file, df, creation_datetime, date_type)

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)
