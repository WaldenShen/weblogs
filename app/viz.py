#/usr/bin/python

import os
import gzip
import json
import luigi
import datetime

from utils import ENCODE_UTF8, SEP
from utils import load_behaviors, create_behavior_db, norm_str

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_ADV = os.path.join(BASEPATH, "data", "adv")


class VizTask(luigi.Task):
    task_namespace = "clickstream"

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    def run(self):
        raise NotImplementedError

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class VizRetentionTask(VizTask):
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

class VizNALTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.datetime.now())

    def run(self):
        create_behavior_db(self.ifile)
        logger.info("Insert records from {} into REDIS".format(self.ifile))

        with self.output().open("wb") as out_file:
            out_file.write("KEY{sep}SUBKEY{sep}VALUE\n".format(sep=SEP))

            for key, behaviors in load_behaviors():
                for name, behavior in behaviors.items():
                    if isinstance(behavior, int):
                        out_file.write("{}\n".format(SEP.join([key, name, str(behavior)])))
                    else:
                        for k, v in behavior.items():
                            try:
                                out_file.write(bytes("{}\n".format(SEP.join([key, k, str(v)])), ENCODE_UTF8))
                            except:
                                out_file.write("{}{}".format(SEP.join([key]), SEP))
                                out_file.write(k.encode(ENCODE_UTF8))
                                out_file.write("{}{}\n".format(SEP, v))

'''
INPUT
============================================
{"creation_datetime": "2016-06", "n_count": 2541630.75, "url_type": "logic1", "url_end": "logic1_\u4fe1\u7528\u5361", "percentage": 0.5983244364174981, "date_type": "month", "chain_length": 4, "url_start": "logic1_\u4fe1\u7528\u5361"}


OUTPUT
============================================
{
  "nodes":[
    {"name":"Myriel","group":1},
    {"name":"Napoleon","group":1},
    {"name":"Mlle.Baptistine","group":1},
    {"name":"Mme.Magloire","group":1},
    {"name":"CountessdeLo","group":1},
    {"name":"Geborand","group":1},
    {"name":"Champtercier","group":1},
    {"name":"Cravatte","group":1},
    {"name":"Count","group":1},
    {"name":"OldMan","group":1}
  ],
  "links":[
    {"source":1,"target":0,"value":1},
    {"source":2,"target":0,"value":2},
    {"source":3,"target":0,"value":3},
    {"source":4,"target":0,"value":4},
    {"source":5,"target":0,"value":5},
    {"source":6,"target":0,"value":6},
    {"source":7,"target":0,"value":7},
    {"source":8,"target":0,"value":8},
    {"source":9,"target":0,"value":9}
  ]
}
'''
class VizCorrelationTask(luigi.Task):
    def run(self):
        global ENCODE_UTF8

        results = {"nodes": [], "links": []}
        with self.output().open("wb") as out_file:
            with gzip.open(self.ifile, "rb") as in_file:
                is_header = True
                for line in in_file:
                    if is_header:
                        is_header = False
                    else:
                        o = json.loads(line.decode(ENCODE_UTF8).strip())
                        node_start, node_end, percentage = o["url_start"], o["url_end"], o["percentage"]
                        
