import crashstatsutils
import json
import jydoop

setupjob = crashstatsutils.dosetupjob([('processed_data', 'json')])

ff21_b4_buildid = '20130423212553'
ff21_b4_signatures = ('mozilla::dom::DocumentBinding::CreateInterfaceObjects(JSContext*, JSObject*, JSObject**)',
                      'JSCompartment::getNewType(JSContext*, js::Class*, js::TaggedProto, JSFunction*)',
                      'JS_GetCompartmentPrincipals(JSCompartment*)',
                      'nsStyleSet::ReparentStyleContext(nsStyleContext*, nsStyleContext*, mozilla::dom::Element*)',
                      'nsFrameManager::ReResolveStyleContext(nsPresContext*, nsIFrame*, nsIContent*, nsStyleChangeList*, nsChangeHint, nsChangeHint, nsRestyleHint, mozilla::css::RestyleTracker&, nsFrameManager::DesiredA11yNotifications, nsTArray<nsIContent*>&, TreeMatchConte...',
                  )

ff19_buildid = '20130215130331'
ff19_signatures = ('TlsGetValue',
                   'InterlockedIncrement',
                   'XPC_WN_Helper_NewResolve',
                   '@0x0 | XPC_WN_Helper_NewResolve',
                   'nsXPConnect::GetXPConnect()',
                   'XPC_WN_NoHelper_Resolve',
               )


def map(k, processed_data, context):
    """
    Group and count by (signature, cpu_info)
    """
    if processed_data is None:
        context.write('unprocessed', 1)
        return

    processed = json.loads(processed_data)
    if processed.get('os_name', None) != 'Windows NT':
        return

    if processed.get('build', None) != ff19_buildid:
        return

    signature = processed.get('signature')
    if signature not in ff19_signatures:
        return

    cpuinfo = processed.get('cpu_info', None)
    if cpuinfo is None:
        return

    context.write((signature, cpuinfo), 1)

combine = jydoop.sumreducer
reduce = jydoop.sumreducer
