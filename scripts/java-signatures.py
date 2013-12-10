import crashstatsutils
import jydoop
import json

setupjob = crashstatsutils.dosetupjob([('processed_data', 'json')])

def map(k, processed_data, context):
    if processed_data is None:
        return

    processed = json.loads(processed_data)
    if processed.get('os_name', None) != 'Windows NT':
        return

    signature = processed.get('signature')
    javapresent = processed.get('dump').find('npjp2') != -1

    context.write((signature, javapresent), 1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer
