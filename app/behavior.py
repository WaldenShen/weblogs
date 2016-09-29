#!/usr/bin/python
# coding=UTF-8

import os
import redis
import gzip
import json
import datetime

from utils import ENCODE_UTF8, INTERVAL


CONN_LOGIN_REDIS = redis.ConnectionPool(host='localhost', port=6379, db=0)
DB_LOGIN_REDIS = redis.Redis(connection_pool=CONN_LOGIN_REDIS)

CONN_INTERVAL_REDIS = redis.ConnectionPool(host="localhost", port=6379, db=1)
DB_INTERVAL_REDIS = redis.Redis(connection_pool=CONN_INTERVAL_REDIS)


def load_history():
    global DB_LOGIN_REDIS

    for cookie_id in DB_LOGIN_REDIS.keys():
        yield cookie_id, json.loads(DB_LOGIN_REDIS.get(cookie_id))

def load_cookie_history(cookie_id):
    global DB_LOGIN_REDIS

    ret = DB_LOGIN_REDIS.get(cookie_id)
    if ret:
        return json.loads(ret)
    else:
        return None

def save_cookie_history(cookie_id, creation_datetime):
    global DB_LOGIN_REDIS

    DB_LOGIN_REDIS.set(cookie_id, json.dumps([creation_datetime.strftime("%Y-%m-%d %H:%M:%S")]))

def create_cookie_history(filepath):
    global ENCODE_UTF8, CONN_LOGIN_REDIS, DB_LOGIN_REDIS

    with gzip.open(filepath, "rb") as in_file:
        for line in in_file:
            o = json.loads(line.decode(ENCODE_UTF8))
            cookie_id, creation_datetime = o["cookie_id"], parse_datetime(o["creation_datetime"])

            history = []
            ret = DB_LOGIN_REDIS.get(cookie_id)
            if ret:
                history = json.loads(ret)
            history.append(creation_datetime.strftime("%Y-%m-%d %H:%M:%S"))

            DB_LOGIN_REDIS.set(cookie_id, json.dumps(history))

    print("The size of db is {}".format(len(DB_LOGIN_REDIS.keys())))
    print("The key of last record is {}".format(cookie_id))

def load_interval():
    global DB_INTERVAL_REDIS

    for cookie_id in DB_INTERVAL_REDIS.keys():
        yield cookie_id, json.loads(DB_INTERVAL_REDIS.get(cookie_id))

def load_cookie_interval(cookie_id):
    global DB_INTERVAL_REDIS

    ret = DB_INTERVAL_REDIS.get(cookie_id)
    if ret:
        return json.loads(ret)
    else:
        return None

def save_cookie_interval(cookie_id, record):
    global DB_INTERVAL_REDIS, INTERVAL

    ret = load_cookie_behavior(cookie_id)
    if ret:
        prev_interval = record[INTERVAL]
        for category_type, info in record.items():
            if category_type == INTERVAL:
                ret[INTERVAL][0] += prev_interval[0]
                ret[INTERVAL][1] += prev_interval[1]
            else:
                for category_key, category_value in info.items():
                    ret.setdefault(category_type,{}).setdefault(category_key, 0)
                    ret[category_type][category_key] += category_value

        DB_INTERVAL_REDIS.set(cookie_id, json.dumps(ret))
    else:
        DB_INTERVAL_REDIS.set(cookie_id, json.dumps(record))
