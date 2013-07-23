# mapreduce job to count the distribution of OS data using the Telemetry
# data that is exported from HBase -> HDFS.  This script is intentionally
# similar to osdistribution.py for comparison purposes.

import json
import random
import jydoop
import telemetryutils

# The setupjob function is used to set up the hadoop job correctly. In
# almost all cases you should use telemetryutils.setupjob (or in the future
# crashstatsutils.setupjob or healthreportutils.setupjob)

setupjob = telemetryutils.hdfs_setupjob

mappertype = telemetryutils.hdfs_mappertype

# The map function is required, and is called once for each incoming record.
# The map function may choose to call context.write(key, value) any number
# of times.
#
# Keys and values are limited to None, strings, ints, floats, and tuples of
# these primitive values.

def map(k, v, cx):
    if random.random() > 0.05:
        return
    j = json.loads(v)
    os = j['info']['OS']
    cx.write(os, 1)

# Combining happens on each map node, and is called with an arbitrary number
# map records which have the same key. The combiner, if present, should output
# a record of the same "schema" as the map function does.
#
# In this case, we combine the records by counting the records.

combine = jydoop.sumreducer

# After all of the map records for a particular key have been
# collected, reduction can count or combine these records (it could
# e.g. produce a histogram or some other complex structure).
# 
# In this case, reduction is the exact same as combining, it just adds
# the map and/or combine results together.

reduce = jydoop.sumreducer
