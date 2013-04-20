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

@FHRMapper(only_major_channels=True)
def map(key, payload, context):
    channel = payload.channel
    build_id = payload.app_build_id

    for error in payload.errors():
        context.write(key, '\t'.join((channel, build_id, error)))

