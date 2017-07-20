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

import os
import resource
import sys


def daemonize():
    """
    Turn the process into a UNIX daemon.

    The goals here are to:
        * detach the process from its controlling terminal.
        * change the working directory to / to avoid holding a reference to any mount points.
        * set umask to 0 to avoid inheriting parent settings.
        * close all open file descriptors.
        * reopen stdin, stdout, and stderr as /dev/null.
    """
    pid = os.fork()
    if pid != 0:
        sys.exit(0)
    os.setsid()
    pid = os.fork()
    if pid != 0:
        sys.exit(0)
    os.chdir("/")
    os.umask(0)
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = 1024
    for fd in range(0, maxfd):
        try:
            os.close(fd)
        except OSError:
            pass
    os.open("/dev/null", os.O_RDWR) # Open stdin as /dev/null
    os.dup2(0, 1)  # Duplicate /dev/null to stdout
    os.dup2(0, 2)  # Duplicate /dev/null to stderr
