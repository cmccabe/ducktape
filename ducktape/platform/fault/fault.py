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

import ducktape.platform.fault.fault_state


class Fault(object):
    """
    The base class for a Fault.
    """

    def __init__(self, name, spec):
        """
        Create a Fault.

        :param name:            A string identifying this fault.
        :param spec:            A fault_spec.FaultSpec object describing this fault.
        """
        self.name = name
        self.spec = spec
        self.state = fault_state.PENDING

    def get_start_time_ms(self):
        return self.spec.start_time_ms

    def get_end_time_ms(self):
        return self.spec.end_time_ms()

    def start(self):
        """
        Start the fault.
        """
        if self.state != fault_state.PENDING:
            raise RuntimeError("Can't start a fault in state '%s'" % str(self.state))
        self.state = fault_state.ACTIVE
        self.log.info("Starting %s" % str(self))
        try:
            self._activate()
        except:
            self.state = fault_state.FINISHED
            raise

    def end(self):
        """
        Deactivate the fault.
        """
        if self.state != fault_state.ACTIVE:
            raise RuntimeError("Can't end a fault in state '%s'" % str(self.state))
        self.state = fault_state.FINISHED
        self.log.info("ending %s" % str(self))
        self._deactivate()

    def _activate(self):
        """
        Activate the fault.  This must be implemented by concrete subclasses.
        """
        raise NotImplemented

    def _deactivate(self):
        """
        Deactivate the fault.  This must be implemented by concrete subclasses.
        """
        raise NotImplemented
