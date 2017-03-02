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


class Fault(object):
    """
    The base class for faults.
    """
    def __init__(self, end_time):
        """
        Create a new fault object.
        :param end_time:        The end time of the fault, in seconds since the epoch.
        """
        self.end_time = end_time


class FaultHandler(object):
    """
    The base class for objects that are able to create faults and repair them.
    """
    def create_fault(self, seed, end_time):
        """
        Create a new fault.
        :param seed:            The random seed between 0 and 2^32 to use in the new fault.
        :param end_time:        The end time of the fault, in seconds since the epoch.
        :returns:               The new fault object.
        """
        raise NotImplemented()

    def repair_fault(self, fault):
        """
        Repair an existing fault.
        :param fault:           The fault object to repair.
        """
        raise NotImplemented()
