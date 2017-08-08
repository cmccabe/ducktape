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

import copy
import threading
import traceback

import math

from ducktape.trogdor import client


class NodeStatus(object):
    """
    The status of a particular trogdor agent node.
    """
    def __init__(self, node_name, last_comms_ms):
        """
        Create a new NodeStatus.

        :param node_name:       The node name.
        :param last_comms_ms:   The last time we communicated with this node, or
                                    0 if there was never any communication.
        """
        self.node_name = node_name
        self.last_comms_ms = last_comms_ms


class NodeManager(object):
    """
    A NodeManager manages a remote trogdor agent node.

    The NodeManager has two main jobs: monitoring the status of nodes, and sending
    faults to nodes.  The NodeManager communicates with the remote agent nodes via
    REST requests.

    If enough time goes past without communication with the remote node, we will
    send a request just to make sure it is still there.  The time period at which
    this will happen is the heartbeat_ms.

    Each NodeManager has its own thread.  The main purpose of this is to allow multiple
    REST requests to be sent at the same time.  This way slow or hanging requests to
    one node do not block requests to another.
    """
    def __init__(self, clock, platform, node, heartbeat_ms):
        """
        Create a new NodeManager.

        :param clock:           The clock object to use.
        :param platform:        The platform object to use.
        :param node:            The platform.Node object this NodeManage is managing.
        :param heartbeat_ms:    The heartbeat period in milliseconds.  We will check
                                on the agent after this period elapses, even if nothing
                                has changed on our side.
        """
        self._clock = clock
        self._platform = platform
        self.log = platform.log
        self._node = node
        self._heartbeat_ms = heartbeat_ms
        self._closing = False
        self._last_comm_attempt_ms = 0
        self._queue_lock = threading.Lock()
        self._fault_queue = []
        self._status_lock = threading.Lock()
        self._status = NodeStatus(self._node.name, 0L)
        self._cond = threading.Condition(lock=self._queue_lock)
        self._thread = threading.Thread(target=self._run)
        self._thread.start()

    def _run(self):
        try:
            fault = None
            while True:
                now = self._clock.get()
                if fault is not None:
                    self._last_comm_attempt_ms = now
                    if self._send_fault(fault, now):
                        fault = None
                next_contact_attempt_ms = self._last_comm_attempt_ms + self._heartbeat_ms
                if now < next_contact_attempt_ms:
                    self._last_comm_attempt_ms = now
                    self._send_heartbeat(now)
                wait_ms = math.max(0, next_contact_attempt_ms - now)
                self._queue_lock.acquire()
                try:
                    if (fault is None) and (len(self._fault_queue) != 0):
                        fault = self._fault_queue.pop(0)
                    if fault is None:
                        self._cond.wait(timeout=wait_ms)
                    if self._closing:
                        return
                finally:
                    self._queue_lock.release()
        except Exception as e:
            self.log.warn("node_manager(%s) thread exiting with error %s" %
                          (self._node.name, traceback.format_exc()))
        finally:
            self._status_lock.acquire()
            try:
                self._status.last_comms_ms = 0
            finally:
                self._status_lock.release()

    def _get_next_required_contact_ms(self):
        """
        Get the next time when we are required to send a message to the agent
        whether or not any faults have been queued.
        """
        self._status_lock.acquire()
        try:
            return self._status.last_comms_ms + self._heartbeat_ms
        finally:
            self._status_lock.release()

    def _send_heartbeat(self, now):
        """
        Get the agent status.

        :param now:     The current time in ms
        """
        try:
            client.get_agent_status(self.log, self.node.hostname, self.node.trogdor_agent_port)
            self._status_lock.acquire()
            try:
                self._status.last_comms_ms = now
            finally:
                self._status_lock.release()
        except:
            self.log.warn("node_manager(%s) unable to contact node: %s" %
                          (self._node.name, traceback.format_exc()))
            return False

    def _send_fault(self, fault, now):
        """
        Send a fault to the node being managed.

        :param fault:   The fault to send.
        :param now:     The current time in ms
        :returns:       True only if the send proceeded without error.
        """
        try:
            req = ...
            client.add_agent_fault(self.log, self.node.hostname, self.node.trogdor_agent_port, req)
            self._status_lock.acquire()
            try:
                self._status.last_comms_ms = now
            finally:
                self._status_lock.release()
            return True
        except:
            self.log.warn("node_manager(%s) unable to create fault on node: %s" %
                          (self._node.name, traceback.format_exc()))
            return False

    def get_status(self):
        """
        Get the status of the node that we are managing.

        :return:        A deep copy of the node status object.  It may be
                        modified without affecting this manager.
        """
        self._status_lock.acquire()
        try:
            return copy.deepcopy(self._status)
        finally:
            self._status_lock.release()

    def begin_shutdown(self):
        """
        Begin closing the NodeManager, but do not wait for the process to complete.
        """
        self._queue_lock.acquire()
        try:
            if self._closing:
                return
            self._closing = True
            self._cond.notify_all()
        finally:
            self._queue_lock.release()

    def shutdown(self):
        """
        Close the NodeManager and block until it has shut down.
        """
        self.begin_shutdown()
        self._thread.join()
