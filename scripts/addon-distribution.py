import crashstatsutils
import jydoop
import json

setupjob = crashstatsutils.dosetupjob([('processed_data', 'json')])

def map(k, processed_data, context):
    if processed_data is None:
        return

    processed = json.loads(processed_data)

    if processed.get('signature', None) != 'mozalloc_abort(char const* const) | NS_DebugBreak | mozilla::plugins::PluginModuleParent::StreamCast(_NPP*, _NPStream*)':
        return

    addons = processed.get('addons', None)
    if addons is None:
        context.write("unavailable", 1)
        return

    holaversion = None
    for aid, version in addons:
        if aid == 'jid1-4P0kohSJxU1qGg@jetpack':
            holaversion = version
            break

    if holaversion is None:
        context.write("notfound,", 1)
    else:
        context.write(holaversion, 1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer
