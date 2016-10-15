#!/usr/bin/python
# coding=UTF-8

import os
import gzip
import json
import luigi
import logging
from datetime import datetime

import networkx as nx
import pygraphviz

from networkx.drawing.nx_agraph import write_dot, read_dot

from utils import parse_raw_page, norm_str, norm_category, is_uncategorized_key
from behavior import load_cookie_history

from utils import ENCODE_UTF8, UNKNOWN, INTERVAL
from utils import LOGIC, LOGIC1, LOGIC2, FUNCTION, INTENTION

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_RAW = os.path.join(BASEPATH, "data", "raw")
BASEPATH_CLUSTER = os.path.join(BASEPATH, "data", "cluster")
BASEPATH_TEMP = os.path.join(BASEPATH, "data", "temp")


class HabitDetectionRawTask(luigi.Task):
    task_namespace = "clickstream"

    ifiles = luigi.ListParameter()
    ofile = luigi.Parameter()

    node = luigi.Parameter()

    def add_edge(self,g, nodes, durations):
        for i, node_start in enumerate(nodes):
            for ii, node_end in enumerate(nodes[i+1:]):
                if node_start != node_end:
                    durations[i+1] /= 1000

                    if durations[i+1] < 10:
                        weight = 0.25
                    else:
                        weight = 0.5 + min(0.5, 0.5*(float(durations[i+ii])/60/2))

                    if g.has_edge(node_start, node_end):
                        g[node_start][node_end]["weight"] += weight
                    else:
                        g.add_weighted_edges_from([(node_start, node_end, weight)])

    def run(self):
        global ENCODE_UTF8
        global LOGIC, LOGIC1, LOGIC2, FUNCTION, INTENTION

        g = nx.Graph()
        for filepath in self.ifiles:
            with gzip.open(filepath, "rb") as in_file:
                is_header = True

                nodes = []
                durations = []
                pre_session_id, pre_logic = None, None
                for line in in_file:
                    if is_header:
                        is_header = False
                    else:
                        info = parse_raw_page(line)
                        if info is None:
                            continue

                        session_id, cookie_id, individual_id, url, creation_datetime,\
                        logic1, logic2, function, intention, logic, logic1_function, logic2_function, logic1_intention, logic2_intention,\
                        duration, active_duration, loading_duration = info

                        if pre_session_id is not None and pre_session_id != session_id:
                            self.add_edge(g, nodes, durations)

                            nodes = []
                            durations = []

                        key = None
                        if self.node == LOGIC1:
                            key = logic1
                        elif self.node == LOGIC:
                            key = logic
                        elif self.node == INTENTION:
                            key = intention
                        elif self.node == FUNCTION:
                            key = function
                        elif self.node == "logic1_intention":
                            key = logic1_intention

                        if not is_uncategorized_key(key.decode(ENCODE_UTF8)):
                            nodes.append(norm_str(key).replace('"', "").decode(ENCODE_UTF8))
                            durations.append(active_duration)

                        pre_session_id, pre_logic = session_id, key

            self.add_edge(g, nodes, durations)
            logger.info("Finish {} with {}, and the size of graph is ({}, {})".format(filepath, self.node, g.number_of_nodes(), g.number_of_edges()))

        folder = os.path.dirname(self.output().fn)
        if not os.path.isdir(folder):
            os.makedirs(folder)

        write_dot(g, self.output().fn)

    def output(self):
        return luigi.LocalTarget(self.ofile)

class CommunityDetectionRawTask(luigi.Task):
    task_namespace = "clickstream"

    ifiles = luigi.ListParameter()
    ofile = luigi.Parameter()

    bvisits = luigi.IntParameter(default=5)

    def run(self):
        global ENCODE_UTF8
        global LOGIC, LOGIC1, LOGIC2, FUNCTION, INTENTION

        g = nx.Graph()
        for filepath in self.ifiles:
            with gzip.open(filepath, "rb") as in_file:
                is_header = True

                for line in in_file:
                    if is_header:
                        is_header = False
                    else:
                        o = json.loads(line.decode(ENCODE_UTF8).strip())
                        cookie_id = o["cookie_id"].replace('"', '')
                        dates = load_cookie_history(cookie_id)
                        if len(dates) < self.bvisits:
                            continue

                        if cookie_id != "cookie_id":
                            history = load_cookie_history(cookie_id)
                            for idx, login_datetime in enumerate(sorted([datetime.strptime(d, "%Y-%m-%d %H:%M:%S") for d in history])):
                                if login_datetime.strftime("%Y-%m-%d") == str(o["creation_datetime"].split(" ")[0]):
                                    break

                            if idx+1 < self.bvisits:
                                continue

                            products, intentions = o[LOGIC1], o[INTENTION]
                            total_count = sum([c for c in products.values()])

                            for shape, item in zip(["triangle", "box"], [products, intentions]):
                                for k, v in item.items():
                                    k, v = norm_str(k), float(v)/total_count

                                    if not is_uncategorized_key(k) and k != "myb2b" and k.find("cub") == -1 and k.find("b2b") == -1 and k.find(u"網銀") == -1 and k.find(u"集團公告") == -1 and k.find(u"選單") == -1:
                                        if not g.has_node(cookie_id):
                                            g.add_node(cookie_id, shape="circle")

                                        if not g.has_node(k):
                                            g.add_node(k, shape=shape)

                                        if g.has_edge(cookie_id, k):
                                            g[cookie_id][k]["weight"] += v
                                        else:
                                            g.add_weighted_edges_from([(cookie_id, k, v)])

            logger.info("Finish {}, and the size of graph is ({}, {})".format(filepath, g.number_of_nodes(), g.number_of_edges()))

        folder = os.path.dirname(self.output().fn)
        if not os.path.isdir(folder):
            os.makedirs(folder)

        write_dot(g, self.output().fn)

    def output(self):
        return luigi.LocalTarget(self.ofile)

class MemberDetectionRawTask(luigi.Task):
    task_namespace = "clickstream"

    ifiles = luigi.ListParameter()
    ofile = luigi.Parameter()

    def run(self):
        global ENCODE_UTF8
        global LOGIC, LOGIC1, LOGIC2, FUNCTION, INTENTION

        relations = {LOGIC1: {}, LOGIC: {}, LOGIC2: {}, INTENTION: {}, "logic1_intention": {}}
        for filepath in self.ifiles:
            with gzip.open(filepath, "rb") as in_file:
                is_header = True

                for line in in_file:
                    if is_header:
                        is_header = False
                    else:
                        o = json.loads(line.decode(ENCODE_UTF8).strip())
                        cookie_id = o["cookie_id"]
                        if cookie_id == "cookie_id":
                            continue

                        for node in [LOGIC1, LOGIC2, LOGIC, INTENTION, "logic1_intention"]:
                            item = o[node]
                            total_count = sum([c for c in item.values()])

                            for k, v in item.items():
                               if not is_uncategorized_key(k) and k != "myb2b" and k.find("cub") == -1 and k.find("b2b") == -1 and k.find(u"網銀") == -1 and k.find(u"集團公告") == -1 and k.find(u"選單") == -1:
                                    if float(v)/total_count > 0.3:
                                        #logger.info((cookie_id, k, float(v), total_count, float(v) / total_count))
                                        relations[node].setdefault(k, set()).add(cookie_id)

        logger.info("There are {} relations to build this graph".format(len(relations)))

        r = {}
        for node, info in relations.items():
            logger.info("Start to merge nodes by {}".format(node))

            for k, members in info.items():
                members = list(members)
                for i, node_start in enumerate(members):
                    for ii, node_end in enumerate(members[i+1:]):
                        key = '"{}" -- "{}"'.format(node_start.encode(ENCODE_UTF8), node_end.encode(ENCODE_UTF8))
                        r.setdefault(key, 0)
                        r[key] += 1

        with self.output().open("wb") as out_file:
            out_file.write("strict graph {\n")
            for k, v in r.items():
                out_file.write("\t{}\t[weight={}];\n".format(k, v))
            out_file.write("}")

    def output(self):
        return luigi.LocalTarget(self.ofile)

class CommunityMergedTask(luigi.Task):
    task_namespace = "clickstream"

    ofile = luigi.Parameter()

    interval = luigi.DateIntervalParameter()

    def requires(self):
        raise NotImplementedError

    def run(self):
        g = nx.Graph()

        for input in self.input():
            sub_g = nx.Graph(read_dot(input.fn))
            g.add_edges_from(combined_graphs_edges(g, sub_g))
            g.add_nodes_from(g.nodes(data=True) + sub_g.nodes(data=True))
            logger.info("Finish {}, and the size of graph is ({}, {})".format(input.fn, g.number_of_nodes(), g.number_of_edges()))

        write_dot(g, self.output().fn)

    def output(self):
        return luigi.LocalTarget(self.ofile)

class HabitDetectionTask(CommunityMergedTask):
    task_namespace = "clickstream"

    node = luigi.Parameter()

    def requires(self):
        global BASEPATH_TEMP
        global BASEPATH_CLUSTER

        for date in self.interval:
            ifiles = []
            for hour in range(0, 24):
                ifiles.append(os.path.join(BASEPATH_TEMP, "page_{}_{:02d}.tsv.gz".format(str(date), hour)))

            ofile = os.path.join(BASEPATH_CLUSTER, "category{}_{}.dot".format(self.node, str(date)))
            yield HabitDetectionRawTask(node=self.node, ifiles=ifiles, ofile=ofile)

class CommunityDetectionTask(CommunityMergedTask):
    task_namespace = "clickstream"

    def requires(self):
        for date in self.interval:
            ifiles = [os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(str(date)))]
            ofile = os.path.join(BASEPATH_CLUSTER, "community_{}.dot".format(str(date)))
            yield CommunityDetectionRawTask(ifiles=ifiles, ofile=ofile)

class MemberDetectionTask(CommunityMergedTask):
    task_namespace = "clickstream"

    def requires(self):
        for date in self.interval:
            ifiles = [os.path.join(BASEPATH_RAW, "cookie_{}.tsv.gz".format(str(date)))]
            ofile = os.path.join(BASEPATH_CLUSTER, "member_{}.dot".format(str(date)))
            yield MemberDetectionRawTask(ifiles=ifiles, ofile=ofile)

def combined_graphs_edges(G, H, weight = 1.0):
    for u,v,hdata in H.edges_iter(data=True):
        # multply attributes of H by weight
        attr = dict( (key, float(value)*weight) for key,value in hdata.items())

        # get data from G or use empty dict if no edge in G
        gdata = {}
        if G.has_node(u):
            gdata = G[u].get(v,{})

        # add data from g
        # sum shared items
        shared = set(gdata) & set(hdata)
        attr.update(dict((key, attr[key] + float(gdata[key])) for key in shared))

        # non shared items
        non_shared = set(gdata) - set(hdata)
        attr.update(dict((key, gdata[key]) for key in non_shared))

        yield u, v, attr

if __name__ == "__main__":
    a = nx.Graph(read_dot("dot1.dot"))
    b = nx.Graph(read_dot("dot2.dot"))

    for g in [a, b]:
        for node in g.nodes():
            print g[node]

        print

    print list(combined_graphs_edges(a, b, weight=1))

    g = nx.Graph()
    g.add_edges_from(combined_graphs_edges(a, b, weight=1))
    for node in g.nodes():
        print g[node]
