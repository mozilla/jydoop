import sys
import json
try:
    import org.apache.hadoop.io.Text as Text;
    import java.lang.System as System
except ImportError: #cpython
    Text = str

def map(key, value, context):
    try:
        payload = json.loads(value)
    except:
        e = sys.exc_info()[0]
        context.write(Text("exception:%s" % e), Text(key))
        return

    record = False
    try:
        ch = payload['chromeHangs']
        if type(ch) == dict:
            record = len(ch['memoryMap']) > 0
    except KeyError:
        pass

    try:
        lw = payload['lateWrites']
        if type(lw) == dict:
            record = record or len(lw['memoryMap']) > 0
    except KeyError:
        pass

    if record:
        outkey = Text(value)
        context.write(outkey, Text())
    
#def reduce(key, values, context):
#    context.write(key, Text())
