try:
    import simplejson as json
    print "Using simplejson for faster json parsing"
except ImportError:
    import json
import testpilotutils
import jydoop
import sys

def map(key, value, context):
    try:
        payload = json.loads(value)
        if "personid" in payload:
           context.write(str(payload["personid"]), 1)
        else:
           context.write("noperson", 1)
    except:
        context.write(key, 1)

combine = jydoop.sumreducer

reduce = jydoop.sumreducer

setupjob = testpilotutils.setupjob
