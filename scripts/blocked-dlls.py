import crashstatsutils
import jydoop
import json
import csv

cutoff = 50 * 2**20

setupjob = crashstatsutils.dosetupjob([('meta_data', 'json'), ('processed_data', 'json')])

def map(k, meta_data, processed_data, context):
    if processed_data is None:
        return

    meta = json.loads(meta_data)
    b = meta.get('BlockedDllList', None)
    if b is None or b == '':
        return

    processed = json.loads(processed_data)
    context.write(k, (processed['signature'], b))

def output(path, results):
    f = open(path, 'w')
    w = csv.writer(f, dialect='excel-tab')
    for k, v in results:
        w.writerow((k,) + v)
    f.close()
