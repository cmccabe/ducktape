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

from ducktape.platform.log import Log

from datetime import datetime
from dateutil.tz import tzlocal
import os
import signal

class BasicLog(Log):
    def __init__(self, file_name):
        super(BasicLog, self).__init__()
        self.fp = open(file_name, 'a+')
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)

    def log(self, level, msg):
        datestr = "{:%FT%T%z}".format(datetime.now(tzlocal()))
        self.fp.write("%s [%s]: %s\n" % (level, datestr, msg))
        self.fp.flush()

    def handle_signal(self, signum, frame):
        self.info("Shutting down %d on signal %s." % (os.getpid(), signum))
        os._exit(1)

    def close(self):
        self.fp.close()
