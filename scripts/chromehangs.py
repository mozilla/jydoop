import sys
import json
import telemetryutils

setupjob = telemetryutils.setupjob

"""Map-only script records everything with a chromehang or a late-write entry."""

def map(key, value, context):
    try:
        payload = json.loads(value)
    except:
        e = str(sys.exc_info()[1])
        context.write(key, "exception:%s for key %s" % (e, key))
        return

    record = False
    try:
        ch = payload['chromeHangs']
        if type(ch) == dict:
            record = len(ch['memoryMap']) > 0
    except KeyError:
        pass

    try:
        lw = payload['lateWrites']
        if type(lw) == dict:
            record = record or len(lw['memoryMap']) > 0
    except KeyError:
        pass

    if record:
        context.write(key, value)
