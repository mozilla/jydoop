import crashstatsutils
import jydoop
import json
from org.python.core.util import StringUtil

setupjob = crashstatsutils.dosetupjob([])
def map(k, context):
    result = context.cx.getCurrentValue()
    meta_data = StringUtil.fromBytes(result.getValue("meta_data", "json"))
    meta = json.loads(meta_data)
    product = meta['ProductName']
    version = meta['Version']
    ispluginhang = meta.get('PluginHang', None) == "1"
    err = 0

    kv = result.getColumnLatest("raw_data", "dump")
    if kv is None:
        err += 1
        dumplen = 0
    else:
        dumplen = kv.getValueLength()

    if "additional_minidumps" in meta:
        extradumps = meta["additional_minidumps"].split(",")
        for extradump in extradumps:
            extrakv = result.getColumnLatest("raw_data", "upload_file_minidump_" + extradump)
            if extrakv is None:
                err += 1
            else:
                extralen = extrakv.getValueLength()
                dumplen += extralen

    context.write(k, (product, version, ispluginhang, dumplen, err))

output = jydoop.outputWithKey
