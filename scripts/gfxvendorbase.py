import crashstatsutils
import json
import pydoop

setupjob = crashstatsutils.dosetupjob([('meta_data', 'json'), ('processed_data', 'json')])

def map(k, meta_data, processed_data, context):
    """
    Group and count by (signature, graphicsvendor)
    """
    if processed_data is None:
        context.write('unprocessed', 1)
        return

    try:
        meta = json.loads(meta_data)
        processed = json.loads(processed_data)
    except:
        context.write('jsonerror', 1)
        return

    if processed.get('os_name', None) != 'Windows NT':
        context.write('notwindows', 1)
        return

    signature = processed.get('signature')
    vendor = meta.get('AdapterVendorID', None)

    context.write((signature, vendor), 1)

combine = pydoop.sumreducer
reduce = pydoop.sumreducer
