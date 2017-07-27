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


import json

from ducktape.basic_platform.basic_log import BasicLog
from ducktape.basic_platform.basic_topology import BasicNode
from ducktape.platform.platform import Platform
from ducktape.platform.topology import Topology


def _get_optional_int(dict, key):
    val = dict.get(key)
    if val is None:
        return val
    return int(val)


def _get_optional_str_list(dict, key):
    rval = []
    val = dict.get(key)
    if val is None:
        return rval
    for item in val:
        rval.append(str(item))
    return rval


def create_platform(config_path, resolvers):
    log = None
    success = False
    try:
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
            trogdor_agent_port = _get_optional_int(node_data, "trogdor_agent_port")
            trogdor_coordinator_port = _get_optional_int(node_data, "trogdor_coordinator_port")
            tags = _get_optional_str_list(node_data, "tags")
            name_to_node[node_name] = BasicNode(node_name, trogdor_agent_port,
                                                trogdor_coordinator_port, tags,
                                                node_data["hostname"])
        topology = Topology(name_to_node)
        platform = BasicPlatform(log, topology, resolvers)
        success = True
        return platform
    finally:
        if not success:
            if log is not None:
                log.close()


class BasicPlatform(Platform):
    """
    Implements the basic platform.
    In this platform, we assume:
    * we can ssh into nodes based on their names.
    * we can invoke iptables to create network partitions
    """
    def __init__(self, log, topology, resolvers):
        """
        Initialize the BasicPlatform object.
        """
        super(BasicPlatform, self).__init__("BasicPlatform", log, topology, resolvers)
