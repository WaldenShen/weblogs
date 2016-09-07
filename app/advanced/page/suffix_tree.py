#!/usr/bin/python

import re
import gzip
import pandas as pd

from pandas import DataFrame

SEP = "\t"

class CommonPath(object):
    def __init__(self):
        self.paths = {}

    def plant_tree(self, session_id, nodes):
        for path in range(0, len(nodes)-1):
            for path2 in range(path, len(nodes)+1):
                if len(nodes[path:path2]) >= 2:
                    key = tuple(nodes[path:path2])

                    self.paths.setdefault(key, set())
                    self.paths[key].add(session_id)

    def print_tree(self, how_many_people=20, how_long_path=6):
        for paths, session_ids in self.paths.items():
            session_ids = list(session_ids)

            if len(session_ids) > how_many_people and len(paths) > how_long_path:
                yield session_ids, paths

if __name__ == "__main__":
    filepath = "../../../data/raw/path_2016-08-01.csv.gz"

    common_path = CommonPath()

    with gzip.open(filepath, "rb") as in_file:
        is_header = True

        for line in in_file:
            if is_header:
                is_header = False
            else:
                session_id, _, _, path = re.split("[\t,]", line.strip())

                nodes = path.split(">")
                if len(nodes) > 7:
                    common_path.plant_tree(session_id, nodes)

    common_path.print_tree()
