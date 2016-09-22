#/usr/bin/python

import os
import gzip
import json
import luigi

from utils import ENCODE_UTF8

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_ADV = os.path.join(BASEPATH, "data", "adv")


class VizRetentionTask(luigi.Task):
    task_namespace = "clickstream"

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    def run(self):
        global BASEPATH_ADV

        results = {}
        with gzip.open(self.ifile) as in_file:
            for line in in_file:
                o = json.loads(line.decode(ENCODE_UTF8).strip())
                count_new_customers = sum(o[key] if key.find("return") > -1 else 0 for key in o.keys())

                results.setdefault(o["login_datetime"], [])
                results[o["login_datetime"]].append(count_new_customers)

                for key in ["return_1", "return_2", "return_3", "return_4", "return_5", "return_6", "return_7", "return_14", "return_21", "return_28", "return_56", "no_return"]:
                    if key.find("return") > -1:
                        results[o["login_datetime"]].append(o[key])

        with self.output().open("wb") as out_file:
            for login_datetime in sorted(results.keys()):
                try:
                    out_file.write(bytes("'{}': [{}],\n".format(login_datetime, ",".join([str(c) for c in results[login_datetime]])), ENCODE_UTF8))
                except:
                    out_file.write("'{}': [{}],\n".format(login_datetime, ",".join([str(c) for c in results[login_datetime]])))

    def output(self):
        return luigi.LocalTarget(self.ofile)

