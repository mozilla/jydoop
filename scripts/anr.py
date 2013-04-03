#make hadoop ARGS="telemetry anr 20130313 20130313 yyyyMMdd" SCRIPT=scripts/anr.py
#python FileDriver.py scripts/anr.py test.data
import sys
import json

# This mapper is just a filter: only records which have an android hang report
# are included in the result.
def map(key, value, context):
    if value.find("androidANR") == -1:
        return
    context.write(key, value)
