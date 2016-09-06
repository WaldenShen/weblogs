#!/usr/bin/python

import os
import gzip
import logging

import luigi
import jaydebeapi as jdbc

from utils import SEP

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

    ifile = luigi.Parameter()
    ofile = luigi.Parameter()

    makder_database = luigi.Parameter(default="FH_TEMP")
    maker_table = luigi.Parameter(default="clickstream_page_corr")

    sql = luigi.Parameter(default="INSERT FH_TEMP.clickstream_page_corr VALUES (?,?,?,?)")

    def run(self):
        connection = get_connection()

        cursor = connection.cursor()
        #cursor.prepare(self.sql)

        for input in [self.ifile]:
            rows = []

            is_header = True
            with gzip.open(input, "rb") as in_file:
                for line in in_file:
                    if is_header:
                        is_header = False
                    else:
                        logger.info(len(str(line).strip().split(",")))
                        rows.append(tuple(str(line).strip().split(",")))

            cursor.executemany(self.sql, rows)
            #cursor.commit()

        cursor.close()
        connection.close()

        with open(self.ofile, "wb") as out_file:
            out_file.write("{}\n".format(len(rows)))

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
            out_file.write("{}\n".format(",".join(self.columns.split(SEP))))

            try:
                for row in cursor.fetchall():
                    try:
                        out_file.write("{}\n".format(SEP.join([str(r) for r in row])))
                    except UnicodeEncodeError:
                        count_error += 1
            except jdbc.Error:
                pass

        # close connection
        connection.close()

        logger.warn("The error count is {}".format(count_error))

    def output(self):
        return luigi.LocalTarget(self.ofile)
