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
from math import floor

from ducktape import __version__ as __ducktape_version__
from ducktape.errors import TimeoutError

import importlib
import time


def wait_until(condition, timeout_sec, backoff_sec=.1, err_msg=""):
    """Block until condition evaluates as true or timeout expires, whichever comes first.

    return silently if condition becomes true within the timeout window, otherwise raise Exception with the given
    error message.
    """
    start = time.time()
    stop = start + timeout_sec
    while time.time() < stop:
        if condition():
            return
        else:
            time.sleep(backoff_sec)

    raise TimeoutError(err_msg)


def package_is_installed(package_name):
    """Return true iff package can be successfully imported."""
    try:
        importlib.import_module(package_name)
        return True
    except:
        return False


def ducktape_version():
    """Return string representation of current ducktape version."""
    return __ducktape_version__


def check_port_number(port):
    """
    Check that a port number is valid.

    :param port:    The port number to check.
    """
    port = int(port)
    if (port < 0) or (port > 65535):
        raise RuntimeError("Invalid port %d" % port)


def get_wall_clock_ms():
    """
    Get the wall-clock time in milliseconds.

    :return:        A long representing the wall-clock time in milliseconds.
    """
    return long(floor(time.time() * 1000))


def wall_clock_ms_to_str(t):
    """
    Convert a wall-clock time in milliseconds to a human-readable time.

    :arg t:         A long representing the wall-clock time in milliseconds.
    :return:        A human-readable date string.
    """
    return time.strftime('%FT%T%z', time.localtime(t / 1000.0))
