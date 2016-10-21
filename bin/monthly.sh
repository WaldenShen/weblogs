#!/bin/sh

BASEPATH=$(dirname $0)
FILEPATH_APP="${BASEPATH}/../app"

MODULE_CLICKSTREAM=clickstream
MODULE_DUMP=dump

today=$(date +%Y-%m)

# export PYTHONPATH
export PYTHONPATH=$PYTHONPATH:./

# for Raw Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.RawTask --mode single --interval ${today} --workers 2

# for Sequence Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.SequenceTask --interval ${today} --workers 1

# for Advanced Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.AdvancedTask --mode single --interval ${today} --workers 12

# for RDB Task
luigi --module ${MODULE_CLICKSTREAM} ${MODULE_CLICKSTREAM}.RDBTask --mode single --interval ${today} --workers 12

wait
