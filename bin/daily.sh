#!/bin/sh

BASEPATH=$(dirname $0)
FILEPATH_APP="${BASEPATH}/../app"

MODULE_CLICKSTREAM=clickstream
MODULE_DUMP=dump

today=$(date +%Y-%m-%d)

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
