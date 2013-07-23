try:
    import simplejson as json
    print "Using simplejson for faster json parsing"
except ImportError:
    import json
import jydoop
import sys

def map(key, value, context):
   context.write(key[0:3], value)

"""
Return True (or non-zero int) if you want to keep the data in HDFS and
not bother outputting it locally.

Useful for large data sets and multi-step jobs.
"""
def skip_local_output():
   return True

combine = jydoop.sumreducer

reduce = jydoop.sumreducer

setupjob = jydoop.setupjob
