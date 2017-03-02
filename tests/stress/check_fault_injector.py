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

import random
import threading

from ducktape.stress.fault import FaultHandler, Fault
from ducktape.stress.fault_injector import FaultInjector
from ducktape.stress.timespan_generator import ConstantTimespanGenerator


class MockFault(Fault):
    def __init__(self, end_time):
        super(MockFault, self).__init__(end_time)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class MockFaultHandler(FaultHandler):
    def __init__(self):
        self.num_faults_created = 0
        self.active_faults = []
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.closing = False

    def create_fault(self, seed, end_time):
        fault = MockFault(end_time)
        self.lock.acquire()
        try:
            self.active_faults.append(fault)
            self.num_faults_created = self.num_faults_created + 1
            self.cond.notify_all()
        finally:
            self.lock.release()
        return fault

    def repair_fault(self, fault):
        self.lock.acquire()
        try:
            self.active_faults.remove(fault)
            self.cond.notify_all()
        finally:
            self.lock.release()

    def wait_for_num_faults_created(self, threshold):
        self.lock.acquire()
        try:
            while True:
                if self.num_faults_created >= threshold:
                    return
                self.cond.wait()
        finally:
            self.lock.release()

    def wait_for_zero_active_faults(self):
        self.lock.acquire()
        try:
            while True:
                if len(self.active_faults) == 0:
                    return
                self.cond.wait()
        finally:
            self.lock.release()


class CheckFaultInjector(object):
    def check_create_fault_injector(self):
        rand = random.Random()
        rand.seed(1234567)
        handler = MockFaultHandler()
        injector = FaultInjector(rand, handler, ConstantTimespanGenerator([0.01]), ConstantTimespanGenerator([0.001]))
        try:
            injector.start()
            handler.wait_for_num_faults_created(10)
        finally:
            injector.shutdown()
        injector.repair_all_active_faults()
        handler.wait_for_zero_active_faults()
