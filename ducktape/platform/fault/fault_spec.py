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
import copy
import json

from ducktape.platform.fault.fault import Fault
from ducktape.utils import util


class FaultSpec(object):
    """
    The base class for a specification describing a fault.
    """

    def __init__(self, args):
        """
        Create a new FaultSpec.

        :param kind:                The kind of fault.
        :param start_ms:            The start time in ms.
        :param duration_ms:         The duration in ms.
        """
        self.kind = util.must_pop_str(args, "kind")
        self.start_ms = util.must_pop_long(args, "start_ms")
        self.duration_ms = util.must_pop_long(args, "duration_ms")

    @property
    def end_ms(self):
        """
        Return the designated end time.
        """
        return self.start_ms + self.duration_ms

    def to_json(self):
        """
        Convert this FaultSpec to JSON.

        :return:                    A JSON string.
        """
        v = self.json_vars()
        return json.dumps(self)

    def json_vars(self):
        """
        Return the vars that should be used to dump this spec to JSON.
        This method can be overridden by subclasses if necessary.
        """
        return copy.deepcopy(vars(self))

    def to_fault(self, name, log, loaders):
        for loader in loaders:
            platform = loader.create(self.kind, Fault, log=log, name=name, spec=self)
            if platform is not None:
                return platform
        loader_packages = [ loader.package_name for loader in loaders ]
        raise RuntimeError("Failed to find fault type '%s' in %s" %
                           (self.kind, ", ".join(loader_packages)))
