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

from ducktape.utils import util


class Node(object):
    def __init__(self, name, trogdor_agent_port, trogdor_coordinator_port, tags):
        self.name = name
        self.trogdor_agent_port = trogdor_agent_port
        self.trogdor_coordinator_port = trogdor_coordinator_port
        self.tags = tags


class Topology(object):
    """
    Represents a cluster topology.
    """
    def __init__(self, name_to_node):
        self.name_to_node = name_to_node

    def node_names(self):
        """
        Return a sorted list of all node names.
        """
        return name_to_node.keys().sorted()

    def get_node(self, name):
        return self.name_to_node.get(name)
