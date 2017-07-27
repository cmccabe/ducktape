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

from datetime import datetime, timedelta

import dateutil
import pytz
from dateutil.tz import tzlocal
from math import floor
import importlib
import re
import time

from ducktape import __version__ as __ducktape_version__
from ducktape.errors import TimeoutError


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


LOCAL_TZ=tzlocal()


def wall_clock_ms_to_str(t):
    """
    Convert a wall-clock time in milliseconds to a human-readable time.

    :arg t:         A long representing the wall-clock time in milliseconds.
    :return:        A human-readable date string.
    """
    ts = datetime.fromtimestamp(t / 1000.0, tz=LOCAL_TZ)
    return ts.strftime('%Y-%m-%dT%H:%M:%S%z')


#def str_to_wall_clock_ms(s):
#    """
#    Convert a wall-clock string into a time in milliseconds since the epoch.
#
#    :arg t:         A long representing the wall-clock time in milliseconds.
#    :return:        A human-readable date string.
#    """
#    s = s.strip()
#    print "WATERMELON: s = '%s', s[-5] = '%s'" % (s, s[-5])
#    if s[-5] == "-":
#        body_str = s[:-5]
#        hour_offset = int(s[-4:-2])
#        minute_offset = int(s[-2:])
#    elif s[-5] == "+":
#        body_str = s[:-5]
#        hour_offset = -int(s[-4:-2])
#        minute_offset = -int(s[-2:])
#    else:
#        raise RuntimeError("No +HHMM or -HHMM timezone offset found at the end.")
#    print "body_str = %s, hour_offset = %d, minute_offset = %d" % (body_str, hour_offset, minute_offset)
#    t = datetime.strptime(body_str, '%Y-%m-%dT%H:%M:%S')
#    t = t.replace(tzinfo=pytz.utc)
#    count = time.mktime(t.utctimetuple())
#    #count = count + (minute_offset * 60.0) + (hour_offset * 3600.0)
#    #count = long(floor(count * 1000.0))
#    return count


duration_regex = re.compile(r'((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?$')
seconds_regex = re.compile(r'(?P<seconds>\d+)$')


def parse_duration_string(str):
    """
    Parse a duration string in the format '<num_hours>h<num_minutes>m<num_seconds>s.

    For example, 1h would map to 1 hour.
    1h30m would map to 1 hour, 30 minutes. etc.

    :str:                   The duration string.
    :returns:               A datetime.timedelta object.
    """
    result = duration_regex.match(str)
    if not result:
        result = seconds_regex .match(str)
        if not result:
            raise ValueError("Unable to parse duration string " + str)
    p = {}
    for (name, param) in result.groupdict().iteritems():
        if param:
            p[name] = int(param)
    return timedelta(**p)


def must_pop(dict, key, error_msg=None):
    """
    Retrieve a key from a dictionary, and remove that key from the dictionary.

    :param dict:            The dictionary.
    :param key:             The key to fetch and remove.
    :param error_msg:
    :return:
    """
    str = dict.get(key)
    if str is not None:
        del dict[key]
        return str
    if error_msg is None:
        raise RuntimeError("Failed to find required key %s" % key)
    else:
        raise RuntimeError(error_msg)
