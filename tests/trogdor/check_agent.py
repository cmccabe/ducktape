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

import json
import os
import random
import tempfile

from ducktape.platform.fault import fault_state
from ducktape.platform.platform import create_platform
from ducktape.trogdor.agent import Agent
from ducktape.trogdor.client import get_agent_status, shutdown_agent, add_agent_fault, get_agent_faults
from ducktape.utils import util
from ducktape.utils.clock import WallClock, MockClock


class AgentTestContext(object):
    def __init__(self, clock):
        self.config_path = None
        self.agent = None
        self.agent_port = 8888
        self.clock = clock

    def __enter__(self):
        self._generate_config_file()
        self.platform = create_platform(self.config_path)
        self.agent = Agent(self.clock, self.platform, self.agent_port)
        self.agent.start()
        def can_get_status():
            while True:
                try:
                    self.get_status()
                    return True
                except:
                    return False
        util.wait_until(can_get_status, 5, backoff_sec=.1, err_msg="Failed to get agent status")
        return self

    def __exit__(self, type, value, traceback):
        if self.config_path is not None:
            os.unlink(self.config_path)
        if self.agent is not None:
            self.agent.shutdown()
            self.agent.wait_for_exit()

    def get_status(self):
        return get_agent_status(self.platform.log, "localhost", self.agent_port)

    def get_agent_faults(self):
        faults = get_agent_faults(self.platform.log, "localhost", self.agent_port)
        rval = {}
        for fault in faults:
            rval[fault["name"]] = fault
        return rval

    def _generate_config_file(self):
        dict = {
            "platform": "basic_platform",
            "log": {
                "path": "/dev/stdout"
            },
            "nodes": {
                "node01": {
                    "hostname": "node01",
                    "trogdor_agent_port": self.agent_port
                }
            }
        }
        self.config_path = "%s%s%s%d" % (tempfile.tempdir, os.sep, "agent.cnf.", random.uniform(0, 100000000))
        with open(self.config_path, "w") as f:
            f.write(json.dumps(dict))
        return self.config_path


def _must_have(dict, key):
    if not dict.get(key):
        raise RuntimeError("Expected to find key %s" % key)


class CheckAgent(object):
    def check_get_agent_status(self):
        with AgentTestContext(clock=WallClock()) as ctx:
            status = ctx.get_status()
            _must_have(status, "started_time_ms")
            _must_have(status, "started_time_str")

    def check_agent_shutdown(self):
        with AgentTestContext(clock=WallClock()) as ctx:
            shutdown_agent(ctx.platform.log, "localhost", ctx.agent_port)
            ctx.agent.wait_for_exit()

    def check_add_retrieve_faults(self):
        with AgentTestContext(clock=WallClock()) as ctx:
            faults = get_agent_faults(ctx.platform.log, "localhost", ctx.agent_port)
            assert len(faults) == 0
            request = {
                "name": "myfault",
                "spec": {
                    "kind": "NoOpFault",
                    "start_ms": 0,
                    "duration_ms": 0,
                }
            }
            add_agent_fault(ctx.platform.log, "localhost", ctx.agent_port, request)
            faults = get_agent_faults(ctx.platform.log, "localhost", ctx.agent_port)
            assert len(faults) == 1
            assert "myfault" == faults[0]["name"]
            spec = faults[0]["spec"]
            assert "NoOpFault" == spec["kind"]
            assert 0 == spec["start_ms"]
            assert 0 == spec["duration_ms"]

    def check_wait_for_faults_to_run(self):
        clock = MockClock(100)
        with AgentTestContext(clock=clock) as ctx:
            faults = ctx.get_agent_faults()
            assert len(faults) == 0
            requests = [
                {
                    "name": "myfault2",
                    "spec": {
                        "kind": "NoOpFault",
                        "start_ms": 200,
                        "duration_ms": 100,
                    }
                },
                {
                    "name": "myfault3",
                    "spec": {
                        "kind": "NoOpFault",
                        "start_ms": 199,
                        "duration_ms": 201,
                    }
                }
            ]
            for request in requests:
                add_agent_fault(ctx.platform.log, "localhost", ctx.agent_port, request)
            faults = ctx.get_agent_faults()
            assert len(faults) == 2
            assert faults["myfault2"]["spec"]["start_ms"] == 200
            assert faults["myfault2"]["spec"]["duration_ms"] == 100
            assert faults["myfault2"]["status"]["state"] == fault_state.PENDING
            assert faults["myfault3"]["spec"]["start_ms"] == 199
            assert faults["myfault3"]["spec"]["duration_ms"] == 201
            assert faults["myfault3"]["status"]["state"] == fault_state.PENDING

            clock.increment(99)
            util.wait_until(lambda: ctx.get_agent_faults()["myfault3"]["status"]["state"] == fault_state.ACTIVE,
                20, backoff_sec=.1, err_msg="Fault3 failed to activate.")
            assert ctx.get_agent_faults()["myfault2"]["status"]["state"] == fault_state.PENDING

            clock.increment(1)
            util.wait_until(lambda: ctx.get_agent_faults()["myfault2"]["status"]["state"] == fault_state.ACTIVE,
                20, backoff_sec=.1, err_msg="Fault2 failed to activate.")
            assert ctx.get_agent_faults()["myfault2"]["status"]["state"] == fault_state.ACTIVE

            clock.increment(100)
            util.wait_until(lambda: ctx.get_agent_faults()["myfault2"]["status"]["state"] == fault_state.FINISHED,
                20, backoff_sec=.1, err_msg="Fault2 failed to deactivate.")
            assert ctx.get_agent_faults()["myfault3"]["status"]["state"] == fault_state.ACTIVE

            clock.increment(100)
            util.wait_until(lambda: ctx.get_agent_faults()["myfault3"]["status"]["state"] == fault_state.FINISHED,
                            20, backoff_sec=.1, err_msg="Fault3 failed to deactivate.")
            assert ctx.get_agent_faults()["myfault2"]["status"]["state"] == fault_state.FINISHED
