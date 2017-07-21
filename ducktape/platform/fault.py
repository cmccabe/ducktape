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

import importlib
import json

from ducktape.utils import util


class Fault(object):
    """ A fault. """
    def __init__(self, log, start_time_ms, end_time_ms, spec):
        """
        Create a new fault.

        :param start_time_ms:           The scheduled start time in ms
        :param end_time_ms:             The scheduled end time in ms
        :param spec:                    A dictionary containing the spec.
        """
        self.log = log
        self.start_time_ms = start_time_ms
        self.end_time_ms = end_time_ms
        self.spec = spec
        self.active = False

    def get_start_time_ms(self):
        return self.start_time_ms

    def get_end_time_ms(self):
        return self.end_time_ms

    def start(self):
        """
        Activate the fault.
        """
        self.active = True
        self.log.info("starting %s" % str(self))

    def end(self):
        """
        Deactivate the fault.
        """
        self.active = False
        self.log.info("ending %s" % str(self))

    def is_active(self):
        """
        Return true if the fault is active.
        """
        return self.active

    def __str__(self):
        dict = {
            'start_time_ms' : self.start_time_ms,
            'end_time_ms' : self.end_time_ms,
            'active' : self.active,
            'spec' : self.spec,
        }
        return "Fault" + json.dumps(dict)


class NoOpFault(Fault):
    TYPE = "noop"

    """ A fault that does nothing. """
    def __init__(self, log, start_time_ms, end_time_ms, spec):
        super(NoOpFault, self).__init__(log, start_time_ms, end_time_ms, spec)

    def start(self):
        super(NoOpFault, self).start()

    def end(self):
        super(NoOpFault, self).end()
