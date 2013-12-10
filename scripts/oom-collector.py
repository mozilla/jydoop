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
    if meta.get('ReleaseChannel', None) != 'nightly':
        return

    if int(meta.get('buildid', '0')[:8]) < 20131127:
        return

    processed = json.loads(processed_data)

    if processed.get('os_name', None) != 'Windows NT':
        return

    signature = processed['signature']
    if signature.startswith('NS_ABORT_OOM'):
        signature = signature + " | " + processed['json_dump']['crashing_thread']['frames'][1]['function']

    largestvmblock = None
    blocksize = processed['json_dump'].get('largest_free_vm_block', None)
    if blocksize is not None:
        blocksize = int(blocksize[2:], 16)
    if 'OOMAllocationSize' in meta or (blocksize is not None and blocksize < cutoff):
        context.write(k[7:], (
            blocksize,
            meta.get('OOMAllocationSize', None),
            meta.get('TotalVirtualMemory', None),
            meta.get('AvailablePhysicalMemory', None),
            meta.get('AvailableVirtualMemory', None),
            meta.get('SystemMemoryUsePercentage', None),
            meta.get('AvailablePageFile', None),
            processed.get('uptime', None),
            signature))

def output(path, results):
    f = open(path, 'w')
    w = csv.writer(f)
    w.writerow(('uuid',
                'SmallestVirtualBlock',
                'OOMAllocationSize',
                'TotalVirtualMemory',
                'AvailablePhysicalMemory',
                'AvailableVirtualMemory',
                'SystemMemoryUsePercentage',
                'AvailablePageFile',
                'uptime',
                'signature'))

    for k, v in results:
        w.writerow((k,) + v)
