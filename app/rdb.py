#!/usr/bin/python

import os
import json
import gzip
import logging

import luigi
import sqlite3
import jaydebeapi as jdbc

from utils import norm_str
from utils import SEP, ENCODE_UTF8

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
                                out_file.write("{}\n".format(SEP.join([str(r) for r in row])))
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

    conn = luigi.Parameter(default="sqlite")
    table = luigi.Parameter(default="stats_page")
    database = luigi.Parameter(default="clickstream.db")

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    def run(self):
        global BASEPATH_SQLLITE, ENCODE_UTF8

        conn = None
        table = None
        if self.conn == "sqlite":
            conn = sqlite3.connect(os.path.join(BASEPATH_SQLLITE, self.database))
            table = self.table
        else:
            conn = get_writing_connection()
            table = "BACC_TEMP.{}".format(self.table)

        cursor = conn.cursor()

        sql = None
        with gzip.open(self.ifile, "rb") as in_file:
            rows = []
            for line in in_file:
                j = json.loads(line.decode(ENCODE_UTF8).strip())

                if sql is None:
                    columns = ",".join(j.keys())

                    sql = "INSERT INTO {table}({columns}) VALUES ({value})".format(table=table, columns=columns, value=",".join(["?" for i in j.keys()]))

                rows.append(tuple([norm_str(v) if isinstance(v, str) or isinstance(v, unicode) else v for v in j.values()]))

            if rows:
                cursor.executemany(sql, rows)

        with self.output().open("wb") as out_file:
            try:
                out_file.write(bytes("{} - {}\n".format(len(rows), sql), ENCODE_UTF8))
            except:
                out_file.write("{} - {}\n".format(len(rows), sql))

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
