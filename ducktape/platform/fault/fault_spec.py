# Copyright 2017 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class FaultSpec(object):
    """
    The base class for a specification describing a fault.
    """

    def __init__(self, kind, start_ms, duration_ms):
        """
        Create a new FaultSpec.

        :param kind:                The kind of fault.
        :param start_ms:            The start time in ms.
        :param duration_ms:         The duration in ms.
        """
        self.kind = kind
        self.start_ms = start_ms
        self.duration_ms = duration_ms

    @property
    def end_time_ms(self):
        """
        Return the designated end time.
        """
        return self.start_ms + self.duration_ms

    def to_json(self):
        return json.dumps(self)


class NoOpFaultSpec(FaultSpec):
    """
    The specification for a NoOpFault.
    """
    def __init__(self, start_ms, duration_ms):
        super(FaultSpec, self).__init__("NoOpFault", start_ms, duration_ms)
