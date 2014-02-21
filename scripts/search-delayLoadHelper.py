import crashstatsutils
import jydoop
import json
import re

setupjob = crashstatsutils.dosetupjob([('processed_data', 'json')])
def map(k, processed_data, context):
    if processed_data is None:
        return

    processed = json.loads(processed_data)

    if processed['product'] != 'Firefox':
        return
    if processed['os_name'] != 'Windows NT':
        return
    build = processed['build']
    if build is None or build[:4] not in ('2013', '2014'):
        return

    jdump = processed.get('json_dump', None)
    if jdump is None:
        context.write(k, (k[7:], 'No json_dump'))
        return

    modules = jdump.get('modules', [])
    if not len(modules):
        return

    exename = None
    twoexe = False
    for module in modules:
        name = module['filename'].lower()
        pdbname = module['debug_file'].lower()
        if name.endswith('.exe'):
            if exename is not None:
                twoexe = True
            else:
                if pdbname != '':
                    exename = pdbname
                else:
                    exename = name

    if exename is None:
        exename = ''

    if not twoexe and exename.startswith('flashplayerplugin') or exename in ('firefox.pdb', 'firefox.exe', 'plugin-container.pdb', 'plugin-container.exe'):
        return

    def hasmodule(name):
        for module in modules:
            if module.get('filename', '').lower() == name:
                return True
        return False

    context.write(k, (k[7:],
                      processed['signature'],
                      exename,
                      ('', 'T')[twoexe],
                      hasmodule('mgrldr.dll'),
                      hasmodule('hydramdh.dll'),
                      hasmodule('safetyldr.dll')))
