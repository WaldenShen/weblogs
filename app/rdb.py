#!/usr/bin/python

import os
import gzip
import logging

import luigi
import jaydebeapi as jdbc

from utils import SEP, ENCODE_UTF8

logger = logging.getLogger('luigi-interface')

BASEPATH = "{}/..".format(os.path.dirname(os.path.abspath(__file__)))
BASEPATH_DRIVER = os.path.join(BASEPATH, "drivers")

def get_connection():
    global BASEPATH_DRIVER

    connection = jdbc.connect('com.teradata.jdbc.TeraDriver',
                              ['jdbc:teradata://88.8.98.214/tmode=ANSI,CLIENT_CHARSET=WINDOWS-950',
                               'i0ac30an',
                               'P@$$w0rd'],
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
            out_file.write(bytes("Insert {} records - {}\n".format(count, self.sql), ENCODE_UTF8))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)

class TeradataTable(luigi.Task):
    task_namespace = "clickstream"

    query = luigi.Parameter()

    ofile = luigi.Parameter()
    columns = luigi.Parameter()

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

        with self.output().open('wb') as out_file:
            out_file.write(bytes("{}\n".format(SEP.join(self.columns.split(","))), ENCODE_UTF8))

            try:
                for row in cursor.fetchall():
                    try:
                        out_file.write(bytes("{}\n".format(SEP.join([str(r) for r in row])), ENCODE_UTF8))
                    except UnicodeEncodeError:
                        count_error += 1
            except jdbc.Error:
                pass

        # close connection
        connection.close()

        logger.warn("The error count is {}".format(count_error))

    def output(self):
        return luigi.LocalTarget(self.ofile, format=luigi.format.Gzip)
