#!/usr/bin/python

import os
import json
import gzip
import logging

import luigi
import sqlite3
import psycopg2
import jaydebeapi as jdbc

from utils import norm_str
from utils import SEP, ENCODE_UTF8, FUNCTION

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_DRIVER = os.path.join(BASEPATH, "drivers")
BASEPATH_SQLLITE = os.path.join(BASEPATH, "data", "sqlite")


def get_connection():
    global BASEPATH_DRIVER

    connection = jdbc.connect('com.teradata.jdbc.TeraDriver',
                              ['jdbc:teradata://88.8.98.214/tmode=ANSI,CLIENT_CHARSET=WINDOWS-950',
                               'i0ac30an',
                               'P@$$w0rd'],
                              ['{}/terajdbc4.jar'.format(BASEPATH_DRIVER),
                               '{}/tdgssconfig.jar'.format(BASEPATH_DRIVER)])

    return connection

def get_psql_connection():
    conn = psycopg2.connect("host='localhost' dbname='clickstream' user='tagging' password='tagging'")
    return conn

def get_writing_connection():
    global BASEPATH_DRIVER

    connection = jdbc.connect('com.teradata.jdbc.TeraDriver',
                              ['jdbc:teradata://88.8.98.214/tmode=ANSI,CLIENT_CHARSET=WINDOWS-950',
                               'NT48174',
                               'S914843!'],
                              ['{}/terajdbc4.jar'.format(BASEPATH_DRIVER),
                               '{}/tdgssconfig.jar'.format(BASEPATH_DRIVER)])

    return connection

class TeradataInsertTable(luigi.Task):
    task_namespace = "clickstream"

    ofile = luigi.Parameter()
    sql = luigi.Parameter()

    def requires(self):
        pass

    def parse_line(self, line):
        return tuple(line.strip().split(SEP))

    def run(self):
        connection = get_connection()
        cursor = connection.cursor()

        count = 0
        for input in self.input():
            rows = []
            is_header = True
            with input.open("rb") as in_file:
                for line in in_file:
                    line = line.decode(ENCODE_UTF8)

                    if is_header:
                        is_header = False
                    else:
                        rows.append(self.parse_line(line))

            cursor.executemany(self.sql, rows)
            count += len(rows)

        cursor.close()
        connection.close()

        with self.output().open("wb") as out_file:
            try:
                out_file.write(bytes("Insert {} records - {}\n".format(count, self.sql), ENCODE_UTF8))
            except:
                out_file.write("Insert {} records - {}\n".format(count, self.sql))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class TeradataTable(luigi.Task):
    task_namespace = "clickstream"

    query = luigi.Parameter()
    batch_size = luigi.IntParameter(default=10000)

    ofile = luigi.Parameter()

    def run(self):
        connection = get_connection()

        cursor = connection.cursor()
        sql = self.query

        count_error = 0
        try:
            logger.info(sql)
            cursor.execute(sql)
        except jdbc.DatabaseError:
            count_error += 1

        columns = [column[0] for column in cursor.description]
        with self.output().open('wb') as out_file:
            try:
                out_file.write(bytes("{}\n".format(SEP.join(columns)), ENCODE_UTF8))
            except:
                out_file.write("{}\n".format(SEP.join(columns)))

            try:
                while True:
                    results = cursor.fetchmany(self.batch_size)

                    if results:
                        for row in results:
                           try:
                                out_file.write(bytes("{}\n".format(SEP.join([str(r) for r in row])), ENCODE_UTF8))
                           except:
                                out_file.write("{}\n".format(SEP.join([r.encode(ENCODE_UTF8) if (isinstance(r, str) or isinstance(r, unicode)) else str(r) for r in row])))
                    else:
                        break
            except jdbc.Error as e:
                logger.warn(e)

        # close connection
        connection.close()

        logger.warn("The error count is {}".format(count_error))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class SqlliteTable(luigi.Task):
    task_namespace = "clickstream"

    conn = luigi.Parameter(default="psql")
    table = luigi.Parameter(default="stats_page")
    database = luigi.Parameter(default="clickstream.db")

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    def run(self):
        global BASEPATH_SQLLITE, ENCODE_UTF8

        conn = None
        table = None
        if self.conn == "psql":
            conn = get_psql_connection()
            table = self.table
        elif self.conn == "sqlite":
            conn = sqlite3.connect(os.path.join(BASEPATH_SQLLITE, self.database))
            table = self.table
        else:
            conn = get_writing_connection()
            table = "BACC_TEMP.{}".format(self.table)

        cursor = conn.cursor()

        sql = None
        total_count = 0
        with gzip.open(self.ifile, "rb") as in_file:
            rows = []
            for line in in_file:
                j = json.loads(line.decode(ENCODE_UTF8).strip())

                if sql is None:
                    columns = ",".join(j.keys())

                    sql = "INSERT INTO {table}({columns}) VALUES ({value})".format(table=table, columns=columns, value=",".join(["%s" for i in j.keys()]))

                if table == "history_cookie":
                    if j["category_type"].find(FUNCTION) == -1:
                        j["category_key"] = norm_str(j["category_key"])
                    else:
                        continue
                elif table in ["stats_session", "stats_cookie"]:
                    if j["category_key"].find(FUNCTION) == -1:
                        j["category_value"] = norm_str(j["category_value"])
                    else:
                        continue
                elif table == "stats_page":
                    if j["url_type"].find(FUNCTION) == -1:
                        j["url"] = norm_str(j["url"])
                    else:
                        continue
                elif table == "adv_pagecorr":
                    if j["url_type"].find(FUNCTION) == -1:
                        j["url_start"] = norm_str(j["url_start"])
                        j["url_end"] = norm_str(j["url_end"])
                    else:
                        continue

                rows.append(tuple([v for v in j.values()]))
                if len(rows) > 1000:
                    cursor.executemany(sql, rows)

                    total_count += len(rows)
                    rows = []

            if rows:
                cursor.executemany(sql, rows)
                total_count += len(rows)

        with self.output().open("wb") as out_file:
            try:
                out_file.write(bytes("{} - {}\n".format(total_count, sql), ENCODE_UTF8))
            except:
                out_file.write("{} - {}\n".format(total_count, sql))

        conn.commit()
        conn.close()

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

if __name__ == "__main__":
    conn = get_connection()

    sql = "SELECT * FROM DP_MCIF_REF.RD_ABT_PROD_CODE;"
    cursor = conn.cursor()
    cursor.execute(sql)
    print([column[0] for column in cursor.description])

    conn.close()
