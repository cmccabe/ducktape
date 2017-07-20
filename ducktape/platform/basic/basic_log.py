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
import socket
import sys
import traceback

import errno

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
        self.signal_handler_thread = threading.Thread(target=self._signal_handler_thread)
        self.signal_handler_thread.setDaemon(True)
        self.signal_handler_thread.start()
        sys.excepthook = self._excepthook
        self.prev_signal_handlers = {}
        self.prev_signal_handlers[signal.SIGINT] = signal.signal(signal.SIGINT, self._handle_signal)
        self.prev_signal_handlers[signal.SIGTERM] = signal.signal(signal.SIGTERM, self._handle_signal)
        self.prev_signal_handlers[signal.SIGUSR1] = signal.signal(signal.SIGUSR1, self._handle_signal)
        self.prev_excepthook = sys.excepthook

    def log(self, level, msg):
        datestr = "{:%FT%T%z}".format(datetime.now(tzlocal()))
        self.lock.acquire()
        try:
            self.fp.write("%s [%s]: %s\n" % (level, datestr, msg))
            self.fp.flush()
        finally:
            self.lock.release()

    def _retry_on_eintr(self, func):
        """
        Retry a function call until we succeed or get an error other than EINTR.
        """
        while True:
            try:
                return func()
            except socket.error, e:
                if e.errno != errno.EINTR:
                    raise

    def _signal_handler_thread(self):
        """
        The main body of the signal handler thread.
        """
        try:
            while True:
                signum = self._retry_on_eintr(lambda: os.read(self.read_fd, 1))
                if signum == chr(0):
                    return
                elif signum == chr(signal.SIGUSR1):
                    self._log_thread_traces()
                else:
                    self.warn("Shutting down %d on signal %d" % (os.getpid(), ord(signum)))
                    os._exit(1)
        except Exception as e:
            self.warn("_signal_handler_thread error: %s" % str(e))
            os._exit(1)

    def _log_thread_traces(self):
        """
        Log a full stack trace for every thread.
        """
        self.info("================ BEGIN STACK TRACES ================")
        for thread_id, stack in sys._current_frames().items():
            self.info("Thread %s" % str(thread_id))
            for file_name, line_num, name, line in traceback.extract_stack(stack):
                self.info('  %s, line %d, %s' % (file_name, line_num, name))
            self.info("")
        self.info("================= END STACK TRACES =================")

    def _handle_signal(self, signum, frame):
        """
        Handle an incoming signal.

        Since this function is a signal handler, we're very limited in what we can do.
        So we simply write the signal number to a pipe, to wake up the signal handler thread.
        """
        self._retry_on_eintr(lambda: os.write(self.write_fd, chr(signum)))

    def _excepthook(self, *info):
        text = "".join(traceback.format_exception(*info))
        self.warn("Uncaught exception: %s" % text)
        os._exit(1)

    def close(self):
        for signum, prev_handler in self.prev_signal_handlers:
            signal.signal(signum, prev_handler)
        os.write(self.write_fd, chr(0))
        self.signal_handler_thread.join()
        self.write_fd.close()
        self.read_fd.close()
        self.fp.close()
