#!/usr/bin/python

import json
import luigi
import logging

from saisyo import RawPath
from utils import get_date_type, ENCODE_UTF8

logger = logging.getLogger('luigi-interface')


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
                #out_file.write(bytes("{}\n".format(json.dumps(d)), ENCODE_UTF8))
                out_file.write("{}\n".format(json.dumps(d)))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)
