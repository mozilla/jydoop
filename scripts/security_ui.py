import json
import jydoop
import telemetryutils

setupjob = telemetryutils.setupjob

"""
Example job to read the SECURITY_UI histogram and output one row for each
entry in the "values" map.

Rows are of the form:
YYYYMMDD<uuid>, bucket, count, channel, os, os_version
"""
def map(key, value, cx):
    try:
        j = json.loads(value)
        if 'info' in j and 'histograms' in j:
            info = j['info']
            histograms = j['histograms']
            if "SECURITY_UI" in histograms:
                h = histograms["SECURITY_UI"]
                channel = info.get("appUpdateChannel", "NA")
                os = info.get("OS", "NA")
                os_ver = info.get("version", "NA")
                if "values" in h:
                   for k in h["values"].keys():
                      output = [key[1:], k, h["values"][k], channel, os, os_ver]
                      cx.write(k,output)
    except:
        return
