try:
    import simplejson as json
    print "Using simplejson for faster json parsing"
except ImportError:
    import json
import telemetryutils
import jydoop
import sys

python_map = map

def map(uid, line, context):
    payload = json.loads(line)
    try:
        i = payload['info']
        channel = i.get('appUpdateChannel', "too_old")
        reason = i['reason']
        buildDate = i['appBuildID'][:8]
    except:
        return

    if (channel != 'nightly'
        or not 'slowSQL' in payload
        or buildDate < '20130607'
        or not 'mainThread' in payload['slowSQL']):
        return

    mainThreadSQL = payload['slowSQL']['mainThread']
    for (query, (count, time)) in mainThreadSQL.iteritems():
        if not 'content-prefs.sqlite' in query:
            continue
        context.write(query, [count, time])

def reduce(key, values, context):
    values = list(values)
    out = values[0] 
    for i in range(1, len(values)):
        inarray = values[i]
        for y in range(0, len(inarray)):
            out[y] += inarray[y]
    context.write(key, out)

combine = reduce

setupjob = telemetryutils.setupjob

def output(path, results):
    f = open(path, 'w')
    for k,v in results:
        [count, time] = v
        f.write("%s\t%s\t%s\n" % (time,count,k))
