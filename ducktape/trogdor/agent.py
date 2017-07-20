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

from datetime import datetime
from dateutil.tz import tzlocal
from threading import Thread
import BaseHTTPServer
import argparse
import json
import os
import sys
import threading

from ducktape.platform.platform import create_platform
from ducktape.utils.daemonize import daemonize


class AgentHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    The handler for HTTP requests to the Trogdor agent.

    REST endpoints:

    GET /status         Return the server status.
        Request:
            <any>
        Response:
            {"started": <TIMESTAMP>}

    GET /faults         Return a list of all the current faults.
        Request:
            <any>
        Response:
            [
              {
                "active": True|False,
                "start_time_ms": <scheduled start time in milliseconds>,
                "end_time_ms": <scheduled end time in milliseconds>,
                "spec": {
                  "type": <fault type>
                  <any other data for this fault type>
                }
              },
              ...
            ]

    PUT /faults         Return a list of all the current faults.
        Request:
            [
              {
                "start_time_ms": <scheduled start time in milliseconds>,
                "end_time_ms": <scheduled end time in milliseconds>,
                "spec": {
                  "type": <fault type>
                  <any other data for this fault type>
                }
              },
              ...
            ]
        Response:
            On success, {}
            On error, {"error": <error message>}

    PUT /shutdown       Shut down the server cleanly.
        Request:
            <any>
        Response:
            {}
    """
    agent = None

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        if p == "/status":
           self.send_str_response(200, self.agent.get_status())
        elif p == "/faults":
            self.send_str_response(200, self.agent.get_faults())
        else:
            self.send_str_response(404, "Unknown path %s\n" % self.path)

    def do_PUT(self):
        p = self.path.strip('/')
        if p == "/shutdown":
            self.send_str_response(200, '{}\n')
            self.agent.shutdown()
        elif p == "/faults":
            try:
                self.agent.add_fault_from_json(self.command)
                self.send_str_response(200, '{}\n')
            except Exception as e:
                values = { 'error': str(e) }
                self.send_str_response(400, json.dumps(values))
        else:
            self.send_str_response(404, "Unknown path %s\n" % self.path)

    def send_str_response(self, status, str):
        self.send_response(status)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(str)

class Agent(object):
    def __init__(self, platform, port):
        self.platform = platform
        self.log = platform.log
        self.port = port
        self.lock = threading.Lock()

        # A condition variable used to wake the fault handler thread.
        self.cond = threading.Condition(lock=self.lock)

        # True only if we are shutting down.  Protected by the lock.
        self.shutdown = False

        # A list of platform.Fault objects.  Protected by the lock.
        self.faults = []

    def start(self):
        """ Run the Trogdor agent. """
        self.start_time = datetime.now(tzlocal())
        self.fault_thread = Thread(target=self._run_fault_thread)
        self.fault_thread.daemon = True
        self.fault_thread.start()
        AgentHttpHandler.agent = self
        self.log.info("Starting agent...")
        self.httpd = BaseHTTPServer.HTTPServer(server_address=('', self.port),
                                               RequestHandlerClass=AgentHttpHandler)
        self.httpd.serve_forever()
        self.fault_thread.join()
        self.log.info("Stopping trogdor agent")

    def _run_fault_thread(self):
        try:
            while True:
                self.lock.acquire()
                try:
                    if (self.shutdown):
                        return
                finally:
                    self.lock.release()
        finally:
            self.httpd.shutdown()

    def shutdown(self):
        self.lock.acquire()
        try:
            if (self.shutdown == True):
                return
            self.log.info("Shutting down %d by request." % os.getpid())
            self.shutdown = True
            self.cond.notify_all()
        finally:
            self.lock.release()

    def get_status(self):
        values = { 'started': "{:%FT%T%z}".format(self.start_time) }
        return json.dumps(values)

    def get_faults(self):
        def _fault_to_dict(fault):
            dict = []
            dict["active"] = fault.is_active()
            dict["start_time_ms"] = fault.start_time_ms
            dict["end_time_ms"] = fault.end_time_ms
            dict["spec"] = fault.spec
            return dict

        self.lock.acquire()
        try:
            out = []
            for fault in self.faults:
                out.append(_fault_to_dict(fault))
        finally:
            self.lock.release()
        return json.dumps(out)

    def add_fault_from_json(self, text):
        fault = self._create_fault_from_json(text)
        self.lock.acquire()
        try:
            self.faults.append(fault)
        finally:
            self.lock.release()

    def _create_fault_from_json(self, text):
        return self._create_fault_from_dict(json.loads(text))

    def _create_fault_from_dict(self, dict):
        def _must_get(dict, key):
            if dict.get(key) is None:
                raise RuntimeError("Failed to set %s" % key)
            rval = dict[key]
            del dict[key]
            return rval

        def _must_get_int(dict, key):
            return int(_must_get(dict, key))

        start_time_ms = _must_get_int(dict, "start_time_ms")
        end_time_ms = _must_get_int(dict, "end_time_ms")
        spec = _must_get(dict, "spec")
        if spec == None:
            raise RuntimeError("No fault spec given.")
        if len(dict) != 0:
            raise RuntimeError("Unknown keys %s" % (dict.keys()))
        return self.platform.create_fault(start_time_ms, end_time_ms, spec)


def main():
    """
    The entry point for trogdor_agent.
    """
    parser = argparse.ArgumentParser(description=
                                     "The agent process for the Trogdor fault injection system.")
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
    agent.start()
