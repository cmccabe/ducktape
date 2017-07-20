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

from ducktape.platform.basic.basic_log import BasicLog
from ducktape.platform.platform import Platform, Fault, Node

import json


def create_platform(config_path):
    with open(config_path) as fp:
        data = json.load(fp)
    log_path = "/dev/stdout"
    log_data = data.get("log")
    if log_data != None:
        if log_data.get("path") != None:
            log_path = log_data.get("path")
    log = BasicLog(log_path)
    if data.get("nodes") == None:
        raise RuntimeError("No 'nodes' stanza was defined in '%s'" % config_path)
    name_to_node = {}
    nodes_data = data.get("nodes")
    for node_name in nodes_data.keys():
        node_data = nodes_data[node_name]
        if node_data.get("hostname") == None:
            raise RuntimeError("No 'hostname' given for node '%s'" % node_name)
        name_to_node[node_name] = BasicNode(node_name, node_data["hostname"], node_data.get("agent_port"))
    return BasicPlatform(log, name_to_node)


class BasicNode(Node):
    """ A node inside a basic platform topology. """
    def __init__(self, name, hostname, agent_port):
        """
        Create a BasicNode.
        :param name:        A string identifying the node.
        :param hostname:    The hostname of the node.
        :param port:        The port of the node.
        """
        super(BasicNode, self).__init__(name, agent_port)
        self.hostname = hostname


class BasicPlatform(Platform):
    """
    Implements the basic platform.
    In this platform, we assume:
    * we can ssh into nodes based on their names.
    * we can invoke iptables to create network partitions
    """
    def __init__(self, log, nodes):
        """
        Initialize the BasicPlatform object.
        :param log:         A ducktape.platform.Log object.
        :param nodes:       A map from strings to lists of ducktape.platform.Node objects.
        """
        super(BasicPlatform, self).__init__("BasicPlatform", log, nodes)

    def create_fault(self, start_time_ms, end_time_ms, spec):
        """
        Create a new fault object.  This does not activate the fault.
        :param type:        The type of fault.
        :param info:        A map containing fault info.
        """
        return Fault(start_time_ms, end_time_ms, spec)
