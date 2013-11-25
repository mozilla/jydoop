import crashstatsutils
import jydoop
import json

setupjob = crashstatsutils.dosetupjob([('meta_data', 'json'), ('processed_data', 'json')])

# Bucket by
buckets = [
    # 64k block up to 16 MB (2 ** 24)
    (2 ** 24, 2 ** 16),
    # 1MB block up to 500MB
    (500 * 2**20, 2**20)
    # everything else is "very big"
]

def map(k, meta_data, processed_data, context):
    if processed_data is None:
        return

    processed = json.loads(processed_data)

    if processed.get('signature').startswith('EMPTY'):
        meta = json.loads(meta_data)
        if 'TotalVirtualMemory' in meta:
            context.write("emptydump", 1)
        return

    if processed.get('os_name', None) != 'Windows NT':
        return

    if 'json_dump' not in processed:
        context.write("nojsondump", 1)
        return

    blocksize = processed['json_dump'].get('largest_free_vm_block', None)
    if blocksize is None:
        context.write("novmblock", 1)
        return

    blocksize = int(blocksize[2:], 16)

    bucketed = None
    for (maxsize, bucket) in buckets:
        if blocksize > maxsize:
            continue
        bucketed = (blocksize / bucket) * bucket
        break
    if bucketed is None:
        bucketed = "big"

    context.write(bucketed, 1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer
