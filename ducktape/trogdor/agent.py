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

from ducktape.platform.fault.fault import Fault
from ducktape.platform.platform import create_platform
from ducktape.utils import util
from ducktape.utils.daemonize import daemonize


class AgentHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    The handler for HTTP requests to the Trogdor agent.

    REST endpoints:

    GET /status         Return the server status.
        Request:
            <any>
        Response:
            {
              "started_time_ms": <timestamp>,
              "started_time_str": <time-string>
            }

    GET /faults         Return a list of all the current faults.
        Request:
            <any>
        Response:
            [
              {
                "name": <fault-name-string>,
                "spec": <fault-spec>,
                "status": <fault-status>
              },
              ...
            ]

    PUT /faults         Create a new fault.
        Request:
            {
              "name": <fault-name-string>,
              "spec": <fault-spec>
            },
        Response:
            On success, {}
            On error, {"error": <error message>}

    PUT /shutdown       Shut down the server cleanly.
        Request:
            <any>
        Response:
            {}

    REST datatypes:

    fault-spec: {
            "kind": <fault-kind>,
            "start_ms": <fault-start-time-in-ms>,
            "duration_ms": <fault-duration-in-ms>,
        }

    fault-status: {
            "state": "pending|active|finished"
        }
    """
    agent = None

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        try:
            if self.path == "/status":
               self._send_success_response(200, self.agent.get_status())
            elif self.path == "/faults":
                self._send_success_response(200, self.agent.get_faults())
            else:
                self._send_success_response(404, "Unknown path %s\n" % self.path)
        except Exception as e:
            self._send_error_response(e)

    def do_PUT(self):
        try:
            if self.path == "/shutdown":
                self.agent.shutdown()
                self._send_success_response(200, '{}\n')
            elif self.path == "/faults":
                text = self.rfile.read(int(self.headers.getheader('content-length', 0)))
                self.agent.log.info("PUT /faults.  text='%s'" % text)
                self.agent.add_fault_from_json(text)
                self._send_success_response(200, '{}\n')
            else:
                self._send_success_response(404, "Unknown path %s\n" % self.path)
        except Exception as e:
            self._send_error_response(e)

    def _send_success_response(self, status, str):
        self.agent.log.trace("HTTP %s %s: status %d: %s" %
                             (self.command, self.path, status, str.strip('\n')))
        self._send_response(status, str)

    def _send_error_response(self, e):
        _, _, tb = sys.exc_info()
        err = str(e) + "\n" + "".join(traceback.format_tb(tb))
        self.agent.log.warn("HTTP %s %s: status %d: %s" % (self.command, self.path, 400, err))
        values = { 'error': str(e) }
        self._send_response(400, json.dumps(values))

    def _send_response(self, status, str):
        try:
            self.send_response(status)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(str)
        except Exception as e:
            self.agent.log.warn("Error sending response to %s %s: %s" %
                                (self.command, self.path, traceback.format_exc()))


class FaultSet(object):
    def __init__(self):
        self.faults_by_start_time = []
        self.faults_by_end_time = []

    def first_fault_to_start(self):
        """
        Return the first fault by start time order.
        """
        if len(self.faults_by_start_time) == 0:
            return None
        return self.faults_by_start_time[0]

    def first_fault_to_end(self):
        """
        Return the first fault by end time order.
        """
        if len(self.faults_by_end_time) == 0:
            return None
        return self.faults_by_end_time[0]

    def add_fault(self, fault):
        self.faults_by_start_time.append(fault)
        self.faults_by_end_time.append(fault)
        self.faults_by_start_time = sorted(self.faults_by_start_time,
                                           key=Fault.start_ms.__get__)
        self.faults_by_end_time = sorted(self.faults_by_end_time,
                                         key=Fault.end_ms.__get__)



def fault_set_in_start_time_order(set):
    """
    A generator which returns the faults in a FaultSet by start time order.
    """
    faults_by_start_time = set.faults_by_start_time
    for fault in faults_by_start_time:
        yield fault


def fault_set_in_end_time_order(set):
    """
    A generator which returns the faults in a FaultSet by end time order.
    """
    faults_by_end_time = set.faults_by_end_time
    for fault in faults_by_end_time:
        yield fault


class Agent(object):
    def __init__(self, platform, port):
        self.platform = platform
        self.log = platform.log
        self.port = port
        self.lock = threading.Lock()

        # A condition variable used to wake the fault handler thread.
        self.cond = threading.Condition(lock=self.lock)

        # True only if we are closing.  Protected by the lock.
        self.closing = False

        # The set of platform.Fault objects.  Protected by the lock.
        self.faults = FaultSet()

    def start(self):
        """ Run the Trogdor agent. """
        self.start_time_ms = util.get_wall_clock_ms()
        AgentHttpHandler.agent = self
        self.httpd = BaseHTTPServer.HTTPServer(server_address=('', self.port),
                                               RequestHandlerClass=AgentHttpHandler)
        self.log.info("Starting agent...")
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
        for fault in fault_set_in_start_time_order(self.faults):
            if fault.start_ms > now:
                next_wakeup = fault.getstart_time_ms()
                break
            if fault.is_pending():
                to_start.append(fault)
        return to_start, next_wakeup

    def get_faults_to_end(self, now):
        next_wakeup = now + 360000
        to_end = []
        for fault in fault_set_in_end_time_order(self.faults):
            if fault.end_ms > now:
                next_wakeup = fault.end_ms
                break
            if fault.is_active():
                to_end.append(fault)
        return to_end, next_wakeup

    def _run_fault_thread(self):
        try:
            while True:
                now = util.get_wall_clock_ms()
                self.lock.acquire()
                try:
                    to_start, next_wakeup = self.get_faults_to_start(now)
                    to_end, next_wakeup2 = self.get_faults_to_end(now)
                finally:
                    self.lock.release()
                next_wakeup = min(next_wakeup, next_wakeup2)
                for fault in to_start:
                    try:
                        fault.start()
                        if fault.end_ms < now:
                            to_end.append(fault)
                    except:
                        self.log.warn("_run_fault_thread: failed to start fault %s: %s" %
                                      (fault.name, traceback.format_exc()))
                for fault in to_end:
                    try:
                        fault.end()
                    except:
                        self.log.warn("_run_fault_thread: got exception when ending " +
                                      "fault %s: %s" % (fault.name, traceback.format_exc()))
                self.lock.acquire()
                try:
                    if (self.closing):
                        return
                    delta = next_wakeup - now
                    self.log.info("waiting for %d ms" % delta)
                    self.cond.wait(delta / 1000.0)
                finally:
                    self.lock.release()
        except Exception as e:
            self.log.warn("_run_fault_thread exiting with error %s" % traceback.format_exc())
        finally:
            self.httpd.shutdown()
            for fault in fault_set_in_start_time_order(self.faults):
                if fault.is_active():
                    fault.end()

    def _run_httpd_thread(self):
        self.httpd.serve_forever()
        self.log.info("Trogdor agent exiting")

    def shutdown(self):
        self.lock.acquire()
        try:
            if (self.closing == True):
                return
            self.log.info("Shutting down %d by request." % os.getpid())
            self.closing = True
            self.cond.notify_all()
        finally:
            self.lock.release()

    def get_status(self):
        values = { 'started_time_ms': self.start_time_ms,
                   'started_time_str': util.wall_clock_ms_to_str(self.start_time_ms)
                 }
        return json.dumps(values)

    def get_faults(self):
        def _fault_to_dict(fault):
            dict = {}
            dict["name"] = str(fault.name)
            dict["spec"] = fault.spec.json_vars()
            dict["status"] = {
                "state": str(fault.state)
            }
            return dict

        self.lock.acquire()
        try:
            out = []
            for fault in fault_set_in_start_time_order(self.faults):
                out.append(_fault_to_dict(fault))
        finally:
            self.lock.release()
        return json.dumps(out)

    def add_fault_from_json(self, text):
        fault = self._create_fault_from_json(text)
        self.lock.acquire()
        try:
            self.faults.add_fault(fault)
            self.cond.notify_all()
        finally:
            self.lock.release()

    def _create_fault_from_json(self, text):
        return self._create_fault_from_dict(json.loads(text))

    def _create_fault_from_dict(self, dict):
        fault_name = dict.get("name")
        if fault_name is None:
            raise RuntimeError("You must supply a fault name.")
        fault_spec_dict = dict.get("spec")
        if fault_spec_dict is None:
            raise RuntimeError("You must supply a fault spec.")
        fault_spec = self.platform.create_fault_spec_from_dict(fault_spec_dict)
        return self.platform.create_fault(fault_name, fault_spec)


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
    node = platform.topology.name_to_node.get(name)
    if (node == None):
        raise RuntimeError("No configuration found for node %s.  Configured " +
                           "node names: %s" % (name, platform.node_names()))
    if node.trogdor_agent_port is None:
        raise RuntimeError("No agent_port specified for node %s" % name)
    platform.log.info("Launching trogdor agent %d with port %d" %
                      (os.getpid(), node.trogdor_agent_port))
    agent = Agent(platform, node.trogdor_agent_port)
    agent.start()
    agent.wait_for_exit()
