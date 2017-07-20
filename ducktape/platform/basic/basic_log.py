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
import sys
import traceback

from ducktape.platform.log import Log

from datetime import datetime
from dateutil.tz import tzlocal
import os
import signal
import threading


class BasicLog(Log):
    """
    A log which is written to a local file.

    This class has locking to prevent messages from different threads from being interleaved.

    We also set up signal handlers to print out the signal which terminated the process when
    a signal occurs.
    """
    def __init__(self, file_name):
        super(BasicLog, self).__init__()
        self.fp = open(file_name, 'a+')
        self.lock = threading.Lock()
        self.read_fd, self.write_fd = os.pipe()
        self.prev_signal_handlers = {}
        self.prev_signal_handlers[signal.SIGINT] = signal.signal(signal.SIGINT, self.handle_signal)
        self.prev_signal_handlers[signal.SIGTERM] = signal.signal(signal.SIGTERM, self.handle_signal)
        self.prev_excepthook = sys.excepthook
        sys.excepthook = self.excepthook
        self.signal_handler_thread = threading.Thread(target=self._signal_handler_thread)
        self.signal_handler_thread.setDaemon(True)
        self.signal_handler_thread.start()

    def log(self, level, msg):
        datestr = "{:%FT%T%z}".format(datetime.now(tzlocal()))
        self.lock.acquire()
        try:
            self.fp.write("%s [%s]: %s\n" % (level, datestr, msg))
            self.fp.flush()
        finally:
            self.lock.release()

    def _signal_handler_thread(self):
        signum = os.read(self.read_fd, 1)
        if (signum != chr(0)):
            self.warn("Shutting down %d on signal %d" % (os.getpid(), ord(signum)))
            os._exit(1)

    def handle_signal(self, signum, frame):
        # Since this function is a signal handler, we're very limited in what we can do.
        # So we simply write the signal number to a pipe, to wake up the signal handler thread.
        os.write(self.write_fd, chr(signum))

    def excepthook(self, *info):
        text = "".join(traceback.format_exception(*info))
        self.warn("Uncaught exception: %s" % text)
        os._exit(1)

    def close(self):
        signal.signal(signal.SIGINT, self.prev_signal_handlers[signal.SIGINT])
        signal.signal(signal.SIGINT, self.prev_signal_handlers[signal.SIGTERM])
        os.write(self.write_fd, chr(0))
        self.signal_handler_thread.join()
        self.write_fd.close()
        self.read_fd.close()
        self.fp.close()
