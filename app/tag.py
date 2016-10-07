#!/usr/bin/python

import os
import json
import gzip
import logging

import luigi
from luigi import date_interval as d

from utils import is_uncategorized_key
from utils import ENCODE_UTF8, SEP

logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.INFO)

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_TAG = os.path.join(BASEPATH, "data", "tag")
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")


class TagOutputTask(luigi.Task):
    task_namespace = "clickstream"

    ifiles = luigi.ListParameter()
    ofile = luigi.Parameter()

    tagtype = luigi.Parameter()

    def run(self):
        global ENCODE_UTF8

        cookie = {}

        for filepath in self.ifiles:
            with gzip.open(filepath, "rb") as in_file:
                for line in in_file:
                    o = json.loads(line.decode(ENCODE_UTF8).strip())

                    cookie.setdefault(o["cookie_id"], {})
                    for k, v in o[self.tagtype].items():
                        if not is_uncategorized_key(k):
                            cookie[o["cookie_id"]].setdefault(k, 0)
                            cookie[o["cookie_id"]][k] += v

            logger.info("Finish {} with {}, the size of cookie is {}".format(filepath, self.tagtype, len(cookie)))

        with self.output().open("wb") as out_file:
            for cid, info in cookie.items():
                total_count = sum(info.values())

                for k, v in info.items():
                    out_file.write("{}{sep}{}{sep}{}\n".format(cid, k.encode(ENCODE_UTF8), float(v)/total_count, sep=SEP))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)


class TagTask(luigi.Task):
    task_namespace = "clickstream"

    interval = luigi.DateIntervalParameter()

    def requires(self):
        global BASEPATH_RAW, BASEPATH_TAG

        ifiles = []
        for date in self.interval:
            interval = d.Date.parse(str(date))
            ifiles.append(os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(str(date))))

        for tagtype in ["logic1", "intention"]:
            ofile = os.path.join(BASEPATH_TAG, "{}_{}.tsv.gz".format(tagtype, str(interval)))
            yield TagOutputTask(ifiles=ifiles, ofile=ofile, tagtype=tagtype)
