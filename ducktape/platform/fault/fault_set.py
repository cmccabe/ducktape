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

from ducktape.platform.fault.fault import Fault


class FaultSet(object):
    def __init__(self):
        self.faults_by_start_time = []
        self.faults_by_end_time = []

    def first_fault_to_start(self):
        """
        Return the first fault by start time order.
        """
        if len(self.faults_by_start_time) == 0:
            return None
        return self.faults_by_start_time[0]

    def first_fault_to_end(self):
        """
        Return the first fault by end time order.
        """
        if len(self.faults_by_end_time) == 0:
            return None
        return self.faults_by_end_time[0]

    def add_fault(self, fault):
        """
        Add a new Fault to the FaultSet.
        """
        self.faults_by_start_time.append(fault)
        self.faults_by_end_time.append(fault)
        self.faults_by_start_time = sorted(self.faults_by_start_time,
                                           key=Fault.start_ms.__get__)
        self.faults_by_end_time = sorted(self.faults_by_end_time,
                                         key=Fault.end_ms.__get__)

    def by_start_time(self):
        """
        A generator which returns the faults in a FaultSet by start time order.
        """
        for fault in self.faults_by_start_time:
            yield fault

    def by_end_time(self):
        """
        A generator which returns the faults in a FaultSet by end time order.
        """
        for fault in self.faults_by_end_time:
            yield fault
