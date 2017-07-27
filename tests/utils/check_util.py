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


from ducktape.utils.util import wait_until, parse_duration_string, wall_clock_ms_to_str
import time


class CheckUtils(object):

    def check_wait_until(self):
        """Check normal wait until behavior"""
        start = time.time()

        wait_until(lambda: time.time() > start + .5, timeout_sec=2, backoff_sec=.1)

    def check_wait_until_timeout(self):
        """Check that timeout throws exception"""
        start = time.time()

        try:
            wait_until(lambda: time.time() > start + 5, timeout_sec=.5, backoff_sec=.1, err_msg="Hello world")
            raise Exception("This should have timed out")
        except Exception as e:
            assert e.message == "Hello world"

    def check_parse_duration_string(self):
        assert 3600 == parse_duration_string("1h").total_seconds()
        assert 3 == parse_duration_string("3s").total_seconds()
        assert 3610 == parse_duration_string("1h10s").total_seconds()
        assert 7439 == parse_duration_string("2h3m59s").total_seconds()
        assert 14 == parse_duration_string("14").total_seconds()

#    def check_wall_clock_ms_to_str(self):
#        wall_clock_ms = str_to_wall_clock_ms("2017-07-24T20:51:56+0000")
#        print "wall_clock_ms = %s" % str(wall_clock_ms)
#        #wall_clock_ms = str_to_wall_clock_ms("2017-07-24T13:32:08-0700")
#        #print "wall_clock_ms = %s" % str(wall_clock_ms)
#        #str = wall_clock_ms_to_str(wall_clock_ms)
#        #t2 = str_to_wall_clock_ms(str)
#        #assert t == t2
#
