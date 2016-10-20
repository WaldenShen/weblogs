#!/bin/sh

BASEPATH=/cms/clickstream/github
FILEPATH_REDIS=${BASEPATH}/data/redis

# start the luigi daemon
luigid --background

# start the redis server
cd ${FILEPATH_REDIS}
redis-server redis_login.conf

# start the psql
