#!/usr/bin/python
# coding=UTF-8

import os
import json
import gzip
import luigi
import logging
import operator

import lda
import pickle
import numpy as np

from utils import ENCODE_UTF8, SEP
from utils import _categorized_url, is_uncategorized_key, load_category, norm_str, is_unwanted, merge_costco

logger = logging.getLogger('luigi-interface')

class LDATask(luigi.Task):
    task_namespace = "clickstream"

    ntopic = luigi.IntParameter(default=16)
    niter = luigi.IntParameter(default=1024)
    nstate = luigi.IntParameter(default=1201)
    ntop = luigi.IntParameter(default=10)
    ntype = luigi.Parameter()

    ifiles = luigi.ListParameter()
    ofile = luigi.Parameter()

    def run(self):
        global ENCODE_UTF8

        nodes = self.ntype.split("#")

        vocab = {}
        for url in load_category().keys():
            for node in nodes:
                v = _categorized_url(url, node)
                if not is_uncategorized_key(v.decode(ENCODE_UTF8)) and not is_unwanted(v.decode(ENCODE_UTF8)):
                    vocab.setdefault(merge_costco(norm_str(v)), len(vocab))

        cookie_ids = []
        columns = set()
        X = []
        for filepath in self.ifiles:
            with gzip.open(filepath, "rb") as in_file:
                for line in in_file:
                    o = json.loads(line.decode(ENCODE_UTF8).strip())
                    for node in nodes:
                        cookie_id, attrs = o["cookie_id"], o[node]
                        if cookie_id == "cookie_id":
                            continue

                        x = np.zeros(len(vocab), dtype=int)
                        for attr, value in attrs.items():
                            if not is_uncategorized_key(attr) and not is_unwanted(attr):
                                pos = vocab[merge_costco(norm_str(attr)).encode(ENCODE_UTF8)]
                                x[pos] += value
                                columns.add(pos)

                        if np.sum(x) > 0:
                            cookie_ids.append(cookie_id)
                            X.append(x)

        X = np.array(X)

        deleted_columns = list(set(vocab.values()) - columns)
        if deleted_columns:
            X = np.delete(X, deleted_columns, axis=1)

        model = lda.LDA(n_topics=self.ntopic, n_iter=self.niter, random_state=self.nstate)
        model.fit(X)

        with self.output().open("wb") as out_file:
            count = np.zeros(self.ntopic, dtype=np.int)
            doc_topic = model.doc_topic_
            for n in range(len(cookie_ids)):
                topic_most_pr = doc_topic[n].argmax()
                count[topic_most_pr] += 1

            for i, topic_dist in enumerate(model.topic_word_):
                topic_words = np.array([v[0] for v in sorted(vocab.items(), key=operator.itemgetter(0))])[np.argsort(topic_dist)][:-self.ntop-1:-1]
                out_file.write("Topic {}({}): {}\n\n".format(i, count[i], SEP.join([t for t in topic_words])))

            out_file.write("#number of cookie: {}".format(len(cookie_ids)))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)
