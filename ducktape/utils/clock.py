# Copyright 2015 Confluent Inc.
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
from math import floor
import time


class Clock(object):
    """
    An object which returns the time.
    """
    def __init__(self):
        pass

    def get(self):
        """
        Return the time in milliseconds.
        """
        raise NotImplemented


class WallClock(Clock):
    """
    An object which returns the wall-clock time.
    """
    def __init__(self):
        super(WallClock, self).__init__()

    def get(self):
        """
        Return the time in milliseconds since the epoch.
        """
        return long(floor(time.time() * 1000))


class MockClock(Clock):
    """
    An object used for testing which returns a pre-programmed time.
    """
    def __init__(self, now):
        super(MockClock, self).__init__()
        self.lock = threading.Lock()
        self.now = long(now)

    def get(self):
        self.lock.acquire()
        try:
            return self.now
        finally:
            self.lock.release()

    def increment(self, amount):
        self.lock.acquire()
        try:
            self.now = self.now + long(amount)
        finally:
            self.lock.release()
