try:
    import simplejson as json
    print "Using simplejson for faster json parsing"
except ImportError:
    import json
import telemetryutils
import jydoop
import sys

def map(uid, line, context):
    payload = json.loads(line)
    try:
        i = payload['info']
        channel = i.get('appUpdateChannel', "too_old")
        OS = i['OS']
        appName = i['appName']
        reason = i['reason']
        osVersion = str(i['version'])
        #only care about major versions
        appVersion = i['appVersion'].split('.')[0]
        arch = i['arch']
        buildDate = i['appBuildID'][:8]
    except:
        return

    if (channel != 'nightly' or appVersion != '24' 
        or not 'slowSQL' in payload or 'k0130512' >= buildDate
        or not 'mainThread' in payload['slowSQL']):
        return

    mainThreadSQL = payload['slowSQL']['mainThread']
    for (query, (count, time)) in mainThreadSQL.iteritems():
        if not 'formhistory.sqlite' in query:
            continue
        context.write(query, count)

combine = jydoop.sumreducer

reduce = jydoop.sumreducer

setupjob = telemetryutils.setupjob

def output(path, results):
    f = open(path, 'w')
    for k, v in results:
        f.write("%d\t%s\n" % (v,k))
