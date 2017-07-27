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


class CheckAgent(object):
    def _generate_config_file(self):
        dict = {
            "platform": "basic_platform",
            "log": {
                "path": "/dev/stdout"
            },
            "nodes": {
                "node01": {
                    "hostname": "node01",
                    "trogdor_agent_port": 8888
                }
            }
        }
        path = "%s%s%s%d" % (tempfile.tempdir, os.sep, "agent.cnf.", random.uniform(0, 100000000))
        with open(path, "w") as f:
            f.write(json.dumps(dict))
        return path

    def check_loader_create(self):
        config_path = self._generate_config_file()
        try:
            platform = create_platform(config_path)
            agent = Agent(platform, 8888)
            #agent.serve_forever()
        finally:
            os.unlink(config_path)
