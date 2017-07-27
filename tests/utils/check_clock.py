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

from ducktape.utils import util
from ducktape.utils.clock import WallClock


class CheckClock(object):
    def check_wall_clock(self):
        """
        Check that the wall clock goes forwards.
        """
        clock = WallClock()
        starting_ms = clock.get()
        util.wait_until(lambda: clock.get() <= starting_ms, timeout_sec=20,
                        backoff_sec=.1,
                        err_msg="The wall clock does not seem to be moving forward.")
