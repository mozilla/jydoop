import org.apache.hadoop.io.Text as Text;
import java.lang.System as System
from com.xhaus.jyson import JysonCodec as json

def map(key, value, context):
    payload = json.loads(value.toString())
    i = payload['info']
    outkey = value #reuse Text class
    outkey.set(i['OS'] + "/" + i['version'])
    outvalue = Text(i['appUpdateChannel'] + i['appBuildID'])
    context.write(outkey, outvalue)

def reduce(key, values, context):
    outstr = ""
    for v in values.iterator():
        outstr += v.toString() + ", "
    context.write(key, Text(outstr))

def test(text):
    f = open("sample")
    map(None, Text(f.readline()), context);
    System.exit(0)
