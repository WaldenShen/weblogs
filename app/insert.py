#!/usr/bin/python

import os
import json
import logging

import luigi

from complex import PageCorrTask
from rdb import TeradataInsertTable

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")


class InsertPageCorrTask(TeradataInsertTable):
    '''
    CREATE TABLE FH_TEMP.clickstream_page_corr (
    url_start VARCHAR(256),
    url_end VARCHAR(256),
    url_type VARCHAR(256),
    date_type CHAR(16),
    creation_datetime CHAR(16),
    n_count INT,
    percentage FLOAT,
    chain_length INT )
    '''

    task_namespace = "clickstream"

    interval = luigi.DateIntervalParameter()

    node_type = luigi.Parameter(default="url")

    lib = luigi.Parameter()
    length = luigi.IntParameter(default=4)

    sql = luigi.Parameter(default="INSERT FH_TEMP.clickstream_page_corr VALUES (?,?,?,?,?,?,?,?)")

    def requires(self):
        ofile = "{}/{}_corr_{}.csv.gz".format(BASEPATH_RAW, self.node_type, self.interval)

        yield PageCorrTask(ofile=ofile, interval=self.interval, node_type=self.node_type, lib=self.lib, length=self.length)

    def parse_line(self, line):
        o = json.loads(line)

        return o["url_start"], o["url_end"], o["url_type"], o["date_type"], o["creation_datetime"], o["count"], o["percentage"], o["chain_length"]
