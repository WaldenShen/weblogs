# weblogs

# How to start the process
1. source bin/activate   # use Python virtualenv
2. cd app
3. export PYTHONPATH=$PYTHONPATH:./

# Start the Luigi Server
luigid --background

# Start the redis Server
cd data/redis
redis-server redis_login.conf

# Start the postgresql Server
service postgresql start

# Start the first task
luigi --module clickstream clickstream.RawTask --interval 2016-09-01-2016-10-01 --mode range --workers 8

# Start the sequence task
luigi --module clickstream clickstream.SequenceTask --interval 2016-09-01-2016-10-01 --workers 1  # Cannot execute by multiple workers

# Start the advanced task
luigi --module clickstream clickstream.AdvancedTask --interval 2016-09-01-2016-10-01 --mode range --workers 8

# Start the RDB task
luigi --module clickstream clickstream.RDBTask --interval 2016-09-10-2016-10-01 --mode range --workers 8

# Start the CMS task
luigi --module clickstream clickstream.CMSTask --interval 2016-07-01-2016-10-01 --workers 3

# Start the Cluster task
luigi --module clickstream cickstream.ClusterTask --interval 2016-09-01-2016-10-01 --workers 3
