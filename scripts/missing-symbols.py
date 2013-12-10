import crashstatsutils
import jydoop
import json
import csv
from collections import namedtuple

setupjob = crashstatsutils.dosetupjob([('processed_data', 'json')])

def map(k, processed_data, context):
    if processed_data is None:
        return

    processed = json.loads(processed_data)

    if processed.get('os_name', None) != 'Windows NT':
        return

    signature = processed['signature']
    sigframes = signature.split(' | ')

    emitted = set()

    jdump = processed['json_dump']
    crashing_thread = jdump.get('crashing_thread', None)
    if crashing_thread is None:
        return

    frames = crashing_thread.get('frames', None)
    if frames is None:
        return

    for frame in frames:
        if not frame.get('missing_symbols', False):
            continue

        # break out of the frames if we're past the frames that are part of
        # the signature or we're off in the weeds
        if 'function' in frame:
            if frame['function'] not in sigframes:
                break
        elif 'module' in frame:
            if signature.find(frame['module']) == -1:
                break
        else:
            break

        module = frame['module']
        moduleversion = None
        moduleid = None
        for m in jdump.get('modules', []):
            if m['filename'] == module:
                moduleversion = m.get('version', 'MISSING')
                moduleid = m.get('debug_id', 'MISSING')
                break

        found = (module, moduleversion, moduleid)
        if found not in emitted:
            emitted.add(found)
            context.write(found, 1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer

Result = namedtuple('Result', ('module', 'version', 'id', 'count'))

def output(path, results):
    results = [Result(module, version, id, count) for ((module, version, id), count) in results]
    results.sort(key=lambda r: r.count, reverse=True)

    f = open(path, 'w')
    w = csv.writer(f)
    for r in results:
        w.writerow(r)
