import sys
import org.apache.hadoop.io.Text as Text;
import java.lang.System as System
from com.xhaus.jyson import JysonCodec as json

def map(key, value, context):
    try:
        payload = json.loads(value)
        i = payload['info']
        if not 'chromeHangs' in payload:
            return
    except:
        e = sys.exc_info()[0]
        context.write(Text("exception:%s" % e), Text(key))
        return
    outkey = Text(value)
    context.write(outkey, Text())
    
def reduce(key, values, context):
    context.write(key, Text())

def test(text):
    f = open("sample")
    map(None, Text(f.readline()), context);
    System.exit(0)
