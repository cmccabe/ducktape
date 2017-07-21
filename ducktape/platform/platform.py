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

import importlib
import json

from ducktape.utils import util


def create_platform(config_path):
    """ Create a platform object from a package name. """
    with open(config_path) as fp:
        data = json.load(fp)
    platform_package = data.get("platform")
    if platform_package == None:
        raise RuntimeError("No 'platform' was configured in '%s'" % config_path)
    elif platform_package == "basic":
        platform_package = "ducktape.platform.basic.basic_platform"
    module = importlib.import_module(platform_package)
    return module.create_platform(config_path)


class Node(object):
    """ A node inside a platform topology. """
    def __init__(self, name, agent_port):
        self.name = name
        if agent_port is not None:
            util.check_port_number(agent_port)
        self.agent_port = agent_port


class Fault(object):
    """ A fault. """
    def __init__(self, start_time_ms, end_time_ms, spec):
        """
        Create a new fault.

        :param start_time_ms:           The scheduled start time in ms
        :param end_time_ms:             The scheduled end time in ms
        :param spec:                    A dictionary containing the spec.
        """
        self.start_time_ms = start_time_ms
        self.end_time_ms = end_time_ms
        self.spec = spec
        self.active = False

    def get_start_time_ms(self):
        return self.start_time_ms

    def get_end_time_ms(self):
        return self.end_time_ms

    def start(self):
        """
        Activate the fault.
        """
        self.active = True

    def end(self):
        """
        Deactivate the fault.
        """
        self.active = False

    def is_active(self):
        """
        Return true if the fault is active.
        """
        return self.active


class Platform(object):
    """ The platform we are running on. """
    def __init__(self, name, log, name_to_node):
        """
        Initialize the platform object.
        :param name:                    A string identifying the platform.
        :param log:                     A platform.Log object.
        :param name_to_node:            Maps node names to platform.Node objects.
        """
        self.name = name
        self.log = log
        self.name_to_node = name_to_node

    def node_names(self):
        return self.names_to_nodes.keys().sorted()

    def create_fault(self, start_time_ms, end_time_ms, spec):
        """
        Create a new fault object.  This does not activate the fault.
        :param type:        The type of fault.
        :param info:        A map containing fault info.
        """
        raise NotImplemented
