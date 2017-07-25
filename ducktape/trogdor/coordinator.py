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

from threading import Thread
import BaseHTTPServer
import argparse
import json
import os
import sys
import threading
import traceback

from ducktape.platform.fault import Fault
from ducktape.platform.platform import create_platform
from ducktape.utils import util
from ducktape.utils.daemonize import daemonize


class AgentHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    The handler for HTTP requests to the Trogdor coordinator.

    REST endpoints:

    GET /status          Return the status of the coordinator.
        Request:
            <any>
        Response:
            {
              "server_start_time_ms": <server start time in ms since the epoch>
            }

    GET /nodes          Return the status of all nodes
        Request:
            <any>
        Response:
            {
              "nodes": {
                <node-name>: {
                  "hostname": <hostname>,
                  "agent_port": <agent_port>,
                  "faults": {
                    "state": pending|active|completed,
                    "start_time_ms": <scheduled start time in ms since the epoch>,
                    "duration_ms": <scheduled duration in ms>,
                    "spec": {
                      "id": <fault-id>,
                      "type": <fault type>
                      <any other data for this fault type>
                    }
                  }
                  "last_contact": <last contact time in ms since the epoch>
                }, ...
              }
            }

    PUT /shutdown       Shut down the coordinator cleanly.
        Request:
            <any>
        Response:
            {}
    """


def main():
    """
    The entry point for trogdor_coordinator.
    """
    parser = argparse.ArgumentParser(description=
                                     "The coordinator process for the Trogdor fault injection system.")
    parser.add_argument("--config", action="store", required=True,
                        help="The configuration file to use.")
    parser.add_argument("--name", action="store", required=True,
                        help="The name of this node.")
    parser.add_argument('--foreground', dest='foreground', action='store_true',
                        help="Run the process in the foreground.")
    parsed_args = vars(parser.parse_args(sys.argv[1:]))

    if not parsed_args.get("foreground") == True:
        daemonize()
    platform = create_platform(parsed_args["config"])
    name = parsed_args["name"]
    node = platform.name_to_node.get(name)
    if (node == None):
        raise RuntimeError("No configuration found for node %s.  Configured " +
                           "node names: %s" % (name, platform.node_names()))
    if node.agent_port is None:
        raise RuntimeError("No agent_port specified for node %s" % name)
    msg = "Launching trogdor agent %d with port %d" % (os.getpid(), node.agent_port)
    print msg
    platform.log.info(msg)
    agent = Agent(platform, node.agent_port)
    agent.serve_forever()
