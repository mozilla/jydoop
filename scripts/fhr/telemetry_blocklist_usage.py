# Copyright 2013 The Mozilla Foundation <http://www.mozilla.org/>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from healthreportutils import (
    FHRMapper,
    setupjob,
)

import jydoop


@FHRMapper(only_major_channels=True)
def map(key, payload, context):
    channel = payload.channel

    keys = ['total']

    telemetry_enabled = payload.telemetry_enabled

    if payload.telemetry_enabled:
        keys.append('telemetry-enabled')
    elif telemetry_enabled is None:
        keys.append('telemetry-no-data')
    else:
        keys.append('telemetry-disabled')

    if payload.telemetry_ever_enabled:
        keys.append('telemetry-ever-enabled')
    else:
        keys.append('telemetry-never-enabled')

    blocklist_enabled = payload.blocklist_enabled
    if blocklist_enabled:
        keys.append('blocklist-enabled')
    elif blocklist_enabled is None:
        keys.append('blocklist-no-data')
    else:
        keys.append('blocklist-disabled')

    if payload.blocklist_ever_enabled:
        keys.append('blocklist-ever-enabled')
    else:
        keys.append('blocklist-never-enabled')

    for key in keys:
        context.write('\t'.join((channel, key)), 1)


combine = reduce = jydoop.sumreducer
