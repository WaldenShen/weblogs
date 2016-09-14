#!/usr/bin/python

import json
import luigi
import logging

from saisyo import RawPath
from advanced.page import suffix_tree
from utils import get_date_type, SEP, NEXT, ENCODE_UTF8

logger = logging.getLogger('luigi-interface')


class CommonPathTask(luigi.Task):
    task_namespace = "clickstream"

    ofile = luigi.Parameter()
    interval = luigi.DateIntervalParameter()

    def requires(self):
        yield RawPath(interval=self.interval)

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

        with self.output().open("wb") as out_file:
            for session_ids, paths in common_path.print_tree():
                out_file.write(bytes("{}\n".format(SEP.join(session_ids)), ENCODE_UTF8))
                out_file.write(bytes("{}\n".format(SEP.join(paths)), ENCODE_UTF8))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class PageCorrTask(RawPath):
    task_namespace = "clickstream"

    ofile = luigi.Parameter()

    node_type = luigi.Parameter()

    lib = luigi.Parameter()
    length = luigi.IntParameter(default=2)

    def run(self):
        pagedict, pagecount = {}, {}

        mod = __import__(self.lib, fromlist=[""])

        df = None
        for input in self.input():
            logger.info("Start to process {}({}, {})".format(input.fn, len(pagedict), len(pagecount)))
            df = mod.luigi_run(input.fn, self.node_type, self.length, pagedict, pagecount)

        with self.output().open("wb") as out_file:
            creation_datetime, date_type = get_date_type(self.output().fn)

            for d in mod.get_json(df, self.node_type, date_type, str(self.interval), self.length):
                out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class RetentionTask(luigi.Task):
    task_namespace = "clickstream"

    lib = luigi.Parameter(default="advanced.cookie.rention")

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
