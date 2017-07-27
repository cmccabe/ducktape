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

import os
import random
import re
import tempfile

from ducktape.basic_platform.basic_log import BasicLog


class CheckBasicLog(object):
    def _generate_random_log_path(self):
        return "%s%s%s%d" % (tempfile.tempdir, os.sep, "basic_log.", random.uniform(0, 100000000))

    def _verify_log_contents(self, log_path, expected_levels, expected_messages):
        with open(log_path) as f:
            lines = f.readlines()
        if len(lines) != len(expected_messages):
            raise RuntimeError("Expected %d log lines, but got %d" %
                               (len(expected_messages), len(lines)))
        for i in range(0, len(expected_levels)):
            line = lines[i]
            components = re.sub(r"\s+", " ", line).split(" ", 2)
            expected_level = expected_levels[i]
            level = components[0].strip()
            if level != expected_level:
                raise RuntimeError("Line %d: expected log level %s, but got log level %s" %
                                   (i, expected_level, level))
            message = components[2].strip()
            expected_message = expected_messages[i]
            if message != expected_message:
                raise RuntimeError("Line %d: expected message '%s', but got message '%s'" %
                                   (i, expected_message, message))

    def check_log_create(self):
        log_path = self._generate_random_log_path()
        log = BasicLog(log_path)
        try:
            levels = [
                log.DEBUG,
                log.INFO,
                log.WARN,
                log.TRACE,
            ]
            messages = [
                "This is the first line.",
                "And this is the second.",
                "Three is the final line.",
                "Trace level line."
            ]
            log.debug(messages[0])
            log.info(messages[1])
            log.warn(messages[2])
            log.trace(messages[3])
        finally:
            log.close()
        self._verify_log_contents(log_path, levels, messages)

    def _grab_line_containing(self, f, contents):
        while True:
            line = f.readline()
            if not line:
                raise RuntimeError("Did not find '%s' anywhere in the log output." % contents)
            if line.find(contents) != -1:
                return

    def check_log_thread_traces(self):
        log_path = self._generate_random_log_path()
        log = BasicLog(log_path)
        try:
            log._log_thread_traces()
        finally:
            log.close()
        with open(log_path) as f:
            self._grab_line_containing(f, "================ BEGIN STACK TRACES ================")
            self._grab_line_containing(f, "check_log_thread_traces")
            self._grab_line_containing(f, "================= END STACK TRACES =================")
