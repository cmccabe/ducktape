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

from ducktape.platform.platform import create_platform
from ducktape.trogdor.agent import Agent
from ducktape.trogdor.client import get_agent_status, shutdown_agent, add_agent_fault, get_agent_faults
from ducktape.utils import util


class AgentTestContext(object):
    def __init__(self):
        self.config_path = None
        self.agent = None
        self.agent_port = 8888

    def __enter__(self):
        self._generate_config_file()
        self.platform = create_platform(self.config_path)
        self.agent = Agent(self.platform, self.agent_port)
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
        with AgentTestContext() as ctx:
            status = ctx.get_status()
            _must_have(status, "started_time_ms")
            _must_have(status, "started_time_str")

    def check_agent_shutdown(self):
        with AgentTestContext() as ctx:
            shutdown_agent(ctx.platform.log, "localhost", ctx.agent_port)
            ctx.agent.wait_for_exit()

    def check_add_retrieve_faults(self):
        with AgentTestContext() as ctx:
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
