#!/usr/bin/python

import json
import gzip

import luigi

from utils import is_uncategorized_key
from utils import ENCODE_UTF8, SEP

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
                    for k, v in o[self.tagtype]:
                        if not is_uncategorized_key(k):
                            cookie[o["cookie_id"]].setdefault(k, 0)
                            cookie[o["cookie_id"]][k] += v

        with self.output().open("wb") as out_file:
            for cid, info in cookie.items():
                for k, v in info.items():
                    out_file.write("{}{sep}{}{sep}{}\n".format(cid, k.decode(ENCODE_UTF8), v, sep=SEP))

    def output(self):
        return LocalTarget(self.ofile, format=luigi.format.Gzip)
