#make hadoop ARGS="telemetry anr 20130313 20130313 yyyyMMdd" SCRIPT=scripts/anr.py
import sys
import json
try:
    import org.apache.hadoop.io.Text as Text;
    import java.lang.System as System
except ImportError: #cpython
    Text = str

def map(key, value, context):
    if value.find("androidANR") == -1:
        return
    outkey = Text(value)
    context.write(outkey, Text())
    
if __name__ == "__main__":
    from FileDriver import map_reduce
    map_reduce(sys.modules[__name__], sys.argv[1])
