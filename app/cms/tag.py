#!/usr/bin/python
# coding=UTF-8

import os
import json
import gzip
import logging

import luigi
from luigi import date_interval as d

from utils import is_uncategorized_key, norm_str
from utils import ENCODE_UTF8, SEP, LOGIC, INTENTION

logger = logging.getLogger('luigi-interface')
logger.setLevel(logging.INFO)

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_TAG = os.path.join(BASEPATH, "data", "tag")
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")
BASEPATH_CMS = os.path.join(BASEPATH, "data", "cms")

class TagOutputTask(luigi.Task):
    task_namespace = "clickstream"

    ifiles = luigi.ListParameter()
    ofile = luigi.Parameter()

    tagtype = luigi.Parameter()

    def run(self):
        global ENCODE_UTF8, BASEPATH_CMS

        cookie = {}
        for filepath in self.ifiles:
            with gzip.open(filepath, "rb") as in_file:
                for line in in_file:
                    o = json.loads(line.decode(ENCODE_UTF8).strip())

                    cookie.setdefault(o["cookie_id"], {})
                    if o["cookie_id"] == "cookie_id":
                        continue

                    for k, v in o[self.tagtype].items():
                        if not is_uncategorized_key(k):
                            k = norm_str(k)

                            if self.tagtype == LOGIC:
                                if k.find(u"信用卡") > -1 and k.find(u"停止") == -1:
                                    subkey = k.split("_")[1]
                                    is_matching = False
                                    for kk in [u"信用卡", u"開卡", u"循環", u"分期付款", u"w@card", u"分期", u"調額", u"w", u"帳單分期", u"調升額度", u"餘額代償", u""]:
                                        if subkey == kk:
                                            is_matching = True
                                            break

                                    if not is_matching:
                                        cookie[o["cookie_id"]].setdefault(k, 0)
                                        cookie[o["cookie_id"]][k] += v

                            else:
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

        for tagtype in [LOGIC, INTENTION]:
            ofile = os.path.join(BASEPATH_TAG, "{}_{}.tsv.gz".format(tagtype, str(interval)))
            yield TagOutputTask(ifiles=ifiles, ofile=ofile, tagtype=tagtype)

class MappingTask(luigi.Task):
    task_namespace = "clickstream"

    ifiles = luigi.ListParameter()
    ofile = luigi.Parameter()

    def run(self):
        global ENCODE_UTF8, BASEPATH_CMS

        relations = set()
        for filepath in self.ifiles:
            with gzip.open(filepath, "rb") as in_file:
                for line in in_file:
                    o = json.loads(line.decode(ENCODE_UTF8))
                    cookie_id, profile_id, creation_datetime = o["cookie_id"], o["individual_id"], o["creation_datetime"]
                    if cookie_id != "cookie_id":
                        relations.add("{}\t{}".format(cookie_id, profile_id))

        with self.output().open("wb") as out_file:
            for relation in relations:
               out_file.write("{}\n".format(relation))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)
