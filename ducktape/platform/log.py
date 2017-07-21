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


class Log(object):
    TRACE = "TRACE"

    DEBUG = "DEBUG"

    INFO = "INFO"

    WARN = "WARN"

    def __init__(self):
        pass

    def log(self, level, msg):
        """
        Log a message.

        :param level:           The log level.
        :param msg:             The message string.
        """
        raise NotImplemented

    def trace(self, msg):
        """
        Log a message at TRACE level.

        :param msg:             The message string.
        """
        self.log(Log.TRACE, msg)

    def debug(self, msg):
        """
        Log a message at DEBUG level.

        :param msg:             The message string.
        """
        self.log(Log.DEBUG, msg)

    def info(self, msg):
        """
        Log a message at INFO level.

        :param msg:             The message string.
        """
        self.log(Log.INFO, msg)

    def warn(self, msg):
        """
        Log a message at WARN level.

        :param msg:             The message string.
        """
        self.log(Log.WARN, msg)

    def close(self):
        """
        Close the log.
        """
        raise NotImplemented
