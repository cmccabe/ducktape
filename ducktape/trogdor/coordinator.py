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

from ducktape.platform.fault import fault_state
from ducktape.platform.fault.fault import Fault
from ducktape.platform.fault.fault_set import FaultSet
from ducktape.platform.logged_http import LoggedHttpHandler
from ducktape.platform.platform import create_platform
from ducktape.trogdor.agent import AgentHttpHandler
from ducktape.utils import util
from ducktape.utils.clock import WallClock
from ducktape.utils.daemonize import daemonize


class CoordinatorHttpHandler(LoggedHttpHandler):
    """
    The handler for HTTP requests to the Trogdor coordinator.

    REST endpoints:

    GET /status          Return the status of the coordinator.
        Request:
            <any>
        Response:
            {
              "started_time_ms": <timestamp>,
              "started_time_str": <time-string>
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
    def handle_get(self):
        if self.path == "/status":
            self.send_success_response(200, self.server.coordinator.get_status())
        elif self.path == "/nodes":
            self.send_success_response(200, self.server.coordinator.get_nodes())
        else:
            self.send_success_response(404, "Unknown path %s\n" % self.path)

    def handle_put(self):
        if self.path == "/shutdown":
            self.server.coordinator.shutdown()
            self.send_success_response(200, '{}\n')
        else:
            self.send_success_response(404, "Unknown path %s\n" % self.path)


class Transmission(object):
    def __init__(self, target_node, fault):
        self.target_node = target_node
        self.fault = fault


class Coordinator(object):
    def __init__(self, clock, platform, port):
        self.clock = clock
        self.platform = platform
        self.log = platform.log
        self.port = port
        self.lock = threading.Lock()

        # A condition variable used to wake the fault handler thread.
        self.fault_handler_cond = threading.Condition(lock=self.lock)

        # A condition variable used to wake the fault handler thread.
        self. = threading.Condition(lock=self.lock)

        # True only if we are closing.  Protected by the lock.
        self.closing = False

        # The set of platform.Fault objects.  Protected by the lock.
        self.faults = FaultSet()

    def start(self):
        """ Run the Trogdor coordinator. """
        self.start_time_ms = self.clock.get()
        self.httpd = BaseHTTPServer.HTTPServer(server_address=('', self.port),
                                               RequestHandlerClass=AgentHttpHandler)
        self.log.info("Starting trogdor coordinator...")
        self.fault_thread = Thread(target=self._run_fault_thread)
        self.fault_thread.start()
        self.httpd_thread = Thread(target=self._run_httpd_thread)
        self.httpd_thread.start()

    def wait_for_exit(self):
        self.fault_thread.join()
        self.httpd_thread.join()

    def get_faults_to_start(self, now):
        next_wakeup = now + 360000
        to_start = []
        for fault in self.faults.by_start_time():
            if fault.start_ms > now:
                next_wakeup = fault.start_ms
                break
            if fault.is_pending():
                to_start.append(fault)
        return to_start, next_wakeup

    def _run_fault_thread(self):
        try:
            while True:
                now = self.clock.get()
                self.lock.acquire()
                try:
                    to_start, next_wakeup = self.get_faults_to_start(now)
                finally:
                    self.lock.release()
                for fault in to_start:
                    target_nodes = fault.target_nodes()
                    fault.state = fault_state.FINISHED
                    for target_node in target_nodes:
                        self.transmit_queue.enqueue(Transmission(0, target_node, fault))
                self.lock.acquire()
                try:
                    if (self.closing):
                        return
                    delta = next_wakeup - now
                    self.log.trace("%s: waiting for %d ms" % (now, delta))
                    self.fault_handler_cond.wait(delta / 1000.0)
                finally:
                    self.lock.release()
        except Exception as e:
            self.log.warn("_run_fault_thread exiting with error %s" % traceback.format_exc())
        finally:
            self.httpd.shutdown()

    def _run_httpd_thread(self):
        self.httpd.serve_forever()
        self.log.info("Trogdor coordinator exiting")

    def _run_forwarder_thread(self):

    def shutdown(self):
        self.lock.acquire()
        try:
            if (self.closing == True):
                return
            self.log.info("Shutting down trogdor coordinator %d by request." % os.getpid())
            self.closing = True
            self.fault_handler_cond.notify_all()
        finally:
            self.lock.release()

    def get_status(self):
        values = { 'started_time_ms': self.start_time_ms,
                   'started_time_str': util.wall_clock_ms_to_str(self.start_time_ms)
                   }
        return json.dumps(values)


def main():
    """
    The entry point for trogdor_agent.
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
    node = platform.topology.name_to_node.get(name)
    if (node == None):
        raise RuntimeError("No configuration found for node %s.  Configured " +
                           "node names: %s" % (name, platform.node_names()))
    if node.trogdor_coordinator_port is None:
        raise RuntimeError("No trogdor_coordinator_port specified for node %s" % name)
    platform.log.info("Launching trogdor coordinator %d with port %d" %
                      (os.getpid(), node.trogdor_coordinator_port))
    coordinator = Coordinator(WallClock(), platform, node.trogdor_coordinator_port)
    coordinator.start()
    coordinator.wait_for_exit()

