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

from urlparse import urlparse
import argparse
import json
import requests
import sys


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


def get_agent_status(hostname, port):
    """
    Get the status of the agent.

    :param hostname:            The agent hostname.
    :param port:                The agent port.
    :return:                    A dictionary containing the agent status.
    """
    url = "http://%s:%d/status" % (hostname, port)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def get_agent_faults(hostname, port):
    """
    Get the faults contained by the agent.

    :param hostname:            The agent hostname.
    :param port:                The agent port.
    :return:                    A list of the agent faults.
    """
    url = "http://%s:%d/faults" % (hostname, port)
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def add_agent_fault(hostname, port, spec):
    """
    Add a new fault to the agent.

    :param hostname:            The agent hostname.
    :param port:                The agent port.
    :param spec:                The new fault spec as a dictionary.
    :return:                    An empty object on success.
    """
    url = "http://%s:%d/faults" % (hostname, port)
    spec_json = json.dumps(spec)
    response = requests.put(url, data=spec_json)
    response.raise_for_status()
    return response.json()


def shutdown_agent(hostname, port):
    """
    Add a new fault to the agent.

    :param hostname:            The agent hostname.
    :param port:                The agent port.
    :param spec:                The new fault spec as a dictionary.
    :return:                    An empty object on success.
    """
    url = "http://%s:%d/shutdown" % (hostname, port)
    response = requests.put(url)
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
    parser.add_argument("--spec", action="store",
                        help="The specification to use for the new fault.")
    parsed_args = vars(parser.parse_args(sys.argv[1:]))
    hostname, port = parse_hostport(parsed_args["agent"])
    if not parsed_args.get("action"):
        raise RuntimeError("You must supply an action.")
    elif parsed_args["action"] == "status":
        ret = get_agent_status(hostname, port)
    elif parsed_args["action"] == "faults":
        ret = get_agent_faults(hostname, port)
    elif parsed_args["action"] == "add_fault":
        spec = parsed_args.get("spec")
        if not spec:
            raise RuntimeError("You must supply a fault specification using --spec.")
        ret = add_agent_fault(hostname, port, spec)
    elif parsed_args["action"] == "shutdown":
        ret = shutdown_agent(hostname, port)
    else:
        raise RuntimeError("Unknown action %s" % (parsed_args["action"]))
    print json.dumps(ret)
