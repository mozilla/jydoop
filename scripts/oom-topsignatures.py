import crashstatsutils
import jydoop
import json

cutoff = 50 * 2**20

setupjob = crashstatsutils.dosetupjob([('meta_data', 'json'), ('processed_data', 'json')])

def map(k, meta_data, processed_data, context):
    if processed_data is None:
        return

    meta = json.loads(meta_data)
    processed = json.loads(processed_data)
    signature = processed['signature']
    allocsize = None
    isoom = False

    if signature.startswith('EMPTY'):
        if 'TotalVirtualMemory' not in meta:
            return
        isoom = True

    if processed.get('os_name', None) != 'Windows NT':
        return

    if 'OOMAllocationSize' in meta:
        isoom = True
        allocsize = int(meta['OOMAllocationSize'])

    if meta.get('Notes', '').find("ABORT: OOM") != -1:
        isoom = True

    if 'json_dump' in processed:
        blocksize = processed['json_dump'].get('largest_free_vm_block', None)
        if blocksize is not None:
            blocksize = int(blocksize[2:], 16)
            if blocksize < cutoff:
                isoom = True

    if not isoom:
        return

    context.write(k, (signature, allocsize))

output = jydoop.outputWithKey
