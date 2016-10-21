#!/bin/sh

BASEPATH=$(dirname $0)
FILEPATH_APP="${BASEPATH}/../app"

MODULE_CLICKSTREAM=clickstream
MODULE_DUMP=dump

FILEPATH_SPARK="/cms/tagging"
MODULE_ETL="${FILEPATH_SPARK}/etl.sh"
MODULE_RECOMMENDER="${FILEPATH_SPARK}/recommender.sh"

today=$(date "--date=-1 day" +%Y-%m-%d)

# export PYTHONPATH
export PYTHONPATH=$PYTHONPATH:./

# for Dump Task
luigi --module cms.${MODULE_DUMP} ${MODULE_DUMP}.DumpAllTask --workers 6 &

# for Raw Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.RawTask --interval ${today} --workers 12

# for Sequence Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.SequenceTask --interval ${today} --workers 1

# for Advanced Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.AdvancedTask --interval ${today} --workers 12

# for RDB Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.RDBTask --interval ${today} --workers 12

# for CMS Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.CMSTask --interval ${today} --workers 3

wait

# Spark Task
$(${MODULE_ETL})
$(${MODULE_RECOMMENDER})
