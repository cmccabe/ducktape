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

import argparse
import json
import requests
import sys

from ducktape.platform.log import StdoutLog, NullLog
from ducktape.utils import util


def parse_hostport(hostport):
    """
    Parse a hostname/port pair.

    :param hostport:            A colon-separated host:port string.
    :return:                    string hostname, int port
    """
    split_start = hostport.find("]")
    colon_find_start = None
    if split_start != -1:
        colon_find_start = split_start
    colon_idx = hostport.find(":", colon_find_start)
    if colon_idx == -1:
        raise RuntimeError("No port specified in '%s'" % hostport)
    host = hostport[:colon_idx]
    port_str = hostport[colon_idx+1:]
    return host, int(port_str)


def get_agent_status(log, hostname, port):
    """
    Get the status of the agent.

    :param hostname:            The agent hostname.
    :param port:                The agent port.
    :return:                    A dictionary containing the agent status.
    """
    url = "http://%s:%d/status" % (hostname, port)
    log.trace("GET %s" % url)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_agent_faults(log, hostname, port):
    """
    Get the faults contained by the agent.

    :param hostname:            The agent hostname.
    :param port:                The agent port.
    :return:                    A list of the agent faults.
    """
    url = "http://%s:%d/faults" % (hostname, port)
    log.trace("GET %s" % url)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def add_agent_fault(log, hostname, port, request):
    """
    Add a new fault to the agent.

    :param hostname:            The agent hostname.
    :param port:                The agent port.
    :param spec:                The new fault spec as a dictionary.
    :return:                    An empty object on success.
    """
    url = "http://%s:%d/faults" % (hostname, port)
    request_json = json.dumps(request)
    log.trace("PUT %s %s" % (url, request_json))
    response = requests.put(url, data=request_json)
    response.raise_for_status()
    return response.json()


def shutdown_agent(log, hostname, port):
    """
    Add a new fault to the agent.

    :param hostname:            The agent hostname.
    :param port:                The agent port.
    :param spec:                The new fault spec as a dictionary.
    :return:                    An empty object on success.
    """
    url = "http://%s:%d/shutdown" % (hostname, port)
    response = requests.put(url)
    log.trace("PUT %s" % url)
    response.raise_for_status()
    return response.json()


def agent_client_main():
    """
    The entry point for the trogdor client agent.
    """
    parser = argparse.ArgumentParser(description=
                                     "A client for the trogdor agent process.")
    parser.add_argument("--agent", action="store", required=True,
                        help="The hostname and port of the trogdor agent.  " +
                        "For example, example.com:8888")
    action_group = parser.add_mutually_exclusive_group()
    action_group.add_argument("--status", action="store_const", const="status", dest="action",
                              help="Get the agent status.")
    action_group.add_argument("--faults", action="store_const", const="faults", dest="action",
                              help="Get the list of faults.")
    action_group.add_argument("--add-fault", action="store_const", const="add_fault", dest="action",
                              help="Add a new fault.")
    action_group.add_argument("--shutdown", action="store_const", const="shutdown", dest="action",
                              help="Shutdown the agent.")
    parser.add_argument("--fault-start-time-ms", action="store",
                        help="The start time for the new fault in ms.")
    parser.add_argument("--fault-end-time-ms", action="store",
                        help="The end time for the new fault in ms.")
    parser.add_argument("--fault-start-time-delta", action="store",
                        help="The delta between now and the start time for the new fault.")
    parser.add_argument("--fault-duration", action="store",
                        help="The duration for the new fault.")
    parser.add_argument("--fault-spec", action="store",
                        help="The specification for the new fault.")
    parser.add_argument("--verbose", action="store_true", help="Output more information.")
    parsed_args = vars(parser.parse_args(sys.argv[1:]))
    hostname, port = parse_hostport(parsed_args["agent"])
    if parsed_args.get("verbose"):
        log = StdoutLog()
    else:
        log = NullLog()
    if not parsed_args.get("action"):
        raise RuntimeError("You must supply an action.")
    elif parsed_args["action"] == "status":
        ret = get_agent_status(log, hostname, port)
    elif parsed_args["action"] == "faults":
        ret = get_agent_faults(log, hostname, port)
    elif parsed_args["action"] == "add_fault":
        spec = parsed_args.get("fault_spec")
        if not spec:
            raise RuntimeError("You must supply a fault specification using --fault-spec.")
        request = {'spec' : json.loads(spec)}
        if not parsed_args.get("fault_start_time_ms"):
            if not parsed_args.get("fault_start_time_delta"):
                raise RuntimeError("You must specify the fault start time via --fault-start-time-ms " +
                                   "or --fault-start-time-delta")
            delta = long(util.parse_duration_string(
                            parsed_args["fault_start_time_delta"]).total_seconds() * 1000)
            request["start_time_ms_delta"] = delta
        else:
            ms = long(parsed_args.get("fault_start_time_ms"))
            request["start_time_ms"] = ms
        if not parsed_args.get("fault_end_time_ms"):
            if not parsed_args.get("fault_duration"):
                raise RuntimeError("You must specify the fault end time via --fault-end-time-ms " +
                                   "or --fault-duration")
            delta = long(util.parse_duration_string(
                            parsed_args["fault_duration"]).total_seconds() * 1000)
            request["duration_ms"] = delta
        else:
            ms = long(parsed_args.get("fault_end_time_ms"))
            request["end_time_ms"] = ms
        ret = add_agent_fault(log, hostname, port, request)
    elif parsed_args["action"] == "shutdown":
        ret = shutdown_agent(log, hostname, port)
    else:
        raise RuntimeError("Unknown action %s" % (parsed_args["action"]))
    print json.dumps(ret)
