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

import threading
import time


class FaultInjector(threading.Thread):
    """
    The FaultInjector manages a background thread which inserts faults into running stress tests.
    """
    def __init__(self, rand, handler, next_fault_timegen, fault_length_timegen):
        """
        Create a new FaultInjector object.

        :param random:                  The random object to use in this fault injector.
                                        By using the same seed to initialize this object, you can
                                        get the same fault injection timing across runs.
        :param handler:                 The fault handler to use.
        :param next_fault_timegen:      Generates the time until the next fault.
        :param fault_length_timegen:    Generates the length of a fault.
        """
        threading.Thread.__init__(self)
        self.rand = rand
        self.handler = handler
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.next_fault_timegen = next_fault_timegen
        self.fault_length_timegen = fault_length_timegen
        self.active_faults = []
        self.exiting = False

    class FaultInjectorEvent(object):
        def __init__(self, name):
            self.name = name

    class NextFaultEvent(FaultInjectorEvent):
        def __init__(self, now):
            super(FaultInjector.NextFaultEvent, self).__init__("next_fault")
            self.now = now

    class RepairFaultEvent(FaultInjectorEvent):
        def __init__(self, fault):
            super(FaultInjector.RepairFaultEvent, self).__init__("repair_fault")
            self.fault = fault

    class ExitEvent(FaultInjectorEvent):
        def __init__(self):
            super(FaultInjector.ExitEvent, self).__init__("exit_event")

    def run(self):
        self.next_fault_time = time.time() + self.next_fault_timegen.generate(self.rand)
        while True:
            self.lock.acquire()
            try:
                event = self.wait_for_next_event()
            finally:
                self.lock.release()
            if event.name == "exit_event":
                return
            elif event.name == "next_fault":
                seed = self.rand.randint(0, 0xffffffff)
                end_time = event.now + self.fault_length_timegen.generate(self.rand)
                fault = self.handler.create_fault(seed, end_time)
                self.lock.acquire()
                try:
                    self.active_faults.append(fault)
                    self.active_faults.sort(key = lambda fault: fault.end_time)
                finally:
                    self.lock.release()
                self.next_fault_time = event.now + self.next_fault_timegen.generate(self.rand)
            elif event.name == "repair_fault":
                self.handler.repair_fault(event.fault)

    def wait_for_next_event(self):
        while True:
            if self.exiting:
                return FaultInjector.ExitEvent()
            now = time.time()
            wait_time = 0xffffffff
            if len(self.active_faults) > 0:
                wait_time = self.active_faults[0].end_time - now
                if wait_time <= 0:
                    fault = self.active_faults.pop(0)
                    return FaultInjector.RepairFaultEvent(fault)
            next_fault_wait_time = self.next_fault_time - now
            if next_fault_wait_time <= 0:
                return FaultInjector.NextFaultEvent(now)
            if next_fault_wait_time < wait_time:
                wait_time = next_fault_wait_time
            self.cond.wait(wait_time)

    def shutdown(self):
        """
        Shuts down the FaultInjector and wait until its thread is joined.
        """
        self.lock.acquire()
        try:
            if not self.exiting:
                self.exiting = True
                self.cond.notify_all()
        finally:
            self.lock.release()
        self.join()

    def repair_all_active_faults(self):
        while True:
            self.lock.acquire()
            try:
                if len(self.active_faults) == 0:
                    break
                fault = self.active_faults.pop(0)
            finally:
                self.lock.release()
            self.handler.repair_fault(fault)

