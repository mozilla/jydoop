# mapreduce job to count the distribution of OS data

import json
import random
import pydoop

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

combine = pydoop.sumreducer

# After all of the map records for a particular key have been
# collected, reduction can count or combine these records (it could
# e.g. produce a histogram or some other complex structure).
# 
# In this case, reduction is the exact same as combining, it just adds
# the map and/or combine results together.

reduce = pydoop.sumreducer
