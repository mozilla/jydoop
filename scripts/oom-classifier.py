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

    def writeOOM():
        tvm = meta.get("TotalVirtualMemory", None)
        if tvm is None:
            wintype = "Unknown"
        else:
            tvm = int(tvm)
            if tvm > 3.5 * 2**30:
                wintype = "win64"
            else:
                wintype = "win32"
        context.write(("OOM", wintype), 1)

    if processed.get('signature').startswith('EMPTY'):
        if 'TotalVirtualMemory' in meta:
            writeOOM()
        else:
            context.write("notwindows", 1)
        return

    if processed.get('os_name', None) != 'Windows NT':
        context.write("notwindows", 1)
        return

    if 'OOMAllocationSize' in meta:
        writeOOM()
        return

    if meta.get('Notes', '').find("ABORT: OOM") != -1:
        writeOOM()
        return

    if 'json_dump' not in processed:
        context.write("unknown", 1)
        return

    blocksize = processed['json_dump'].get('largest_free_vm_block', None)
    if blocksize is None:
        context.write("unknown", 1)
        return

    blocksize = int(blocksize[2:], 16)
    if blocksize < cutoff:
        writeOOM()
        return

    context.write("probably-not-OOM", 1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer
