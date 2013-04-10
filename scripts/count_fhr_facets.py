import json
import jydoop
import healthreportutils

setupjob = healthreportutils.setupjob

combine = jydoop.sumreducer

def map(key, value, context):
    try:
        payload = json.loads(value)
    except:
        context.write("Bogus\tBogus\tBogus\tBogus", 1)
        return

    output = []
    try:
        info = payload['geckoAppInfo']
        if type(info) == dict:
            output.append(info['name'])
            output.append(info['os'])
            output.append(info['updateChannel'])
            output.append(info['version'])
    except KeyError:
        pass

    if len(output) == 4:
        outkey = "\t".join(output)
        context.write(outkey, 1)

reduce = jydoop.sumreducer
