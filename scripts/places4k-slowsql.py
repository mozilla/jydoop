try:
    import simplejson as json
    print "Using simplejson for faster json parsing"
except ImportError:
    import json
import telemetryutils
import jydoop
import sys

def hsum(histograms, name):
    if not name in histograms:
        return 0
    h = histograms[name]
    if not 'sum' in h:
        return
    return h['sum']

#deal with shadowing below :(
python_map = map

def reportSQL(context,page_size,ls):
    for (query, (count, time)) in ls.iteritems():
        context.write(page_size, [count, time])

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
        histograms = payload['histograms']
    except:
        return


    if not 'slowSQL' in payload or not 'PLACES_DATABASE_PAGESIZE_B' in histograms:
        return


    h = histograms['PLACES_DATABASE_PAGESIZE_B']
    if not 'values' in h:
        return
    h = h['values']
    page_size = 0
    for k,v in h.iteritems():
        if v != 0:
            page_size = int(k)
            break
    if page_size == 0:
        return

    slowSQL = payload['slowSQL']

    if 'mainThreadSQL' in slowSQL:
        mainThreadSQL = slowSQL['mainThread']
        reportSQL(context, page_size, mainThread)
    if 'otherThreads' in slowSQL:
        otherThreads = slowSQL['otherThreads']
        reportSQL(context, page_size, otherThreads)

def combine(key, values, context):
    values = list(values)
    out = values[0]
    for i in range(1, len(values)):
        inarray = values[i]
        for y in range(0, len(inarray)):
            out[y] += inarray[y]
    context.write(key, out)

reduce = combine

setupjob = telemetryutils.setupjob


def output(path, results):
    f = open(path, 'w')
    for k, v in results:
        f.write("%d,%s\n" % (k, ",".join(python_map(str, v))))
