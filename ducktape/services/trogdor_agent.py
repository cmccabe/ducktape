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
import os.path

from ducktape.services.service import Service


class TrogdorAgentService(Service):
    """
    A ducktape service for running the trogdor fault injection daemon.

    Attributes:
        DEFAULT_ROOT        The default root filesystem path to store service files under.
        DEFAULT_PORT        The default port to use for trogdor_agent daemons.
    """

    DEFAULT_ROOT="/mnt"
    DEFAULT_PORT=8888

    def __init__(self, context, num_nodes, root=DEFAULT_ROOT, port=DEFAULT_PORT):
        """
        Create a trogdor_agent service.

        :param context:     The test context.
        :param num_nodes:   The number of nodes.
        :param root:        The root filesystem path to use for storing service files.
        :param port:        The port to use for the trogdor_agent daemons.
        """
        Service.__init__(self, context, num_nodes)
        self.root = root
        self.port = port
        self.log_path = self._make_path("trogdor_agent.log")
        self.config_path = self._make_path("trogdor_agent.conf")

    def _make_path(self, suffix):
        """
        Create a path under the service root directory.

        :param suffix:      The path suffix to use.
        :return:            The path.
        """
        return "%s%s%s" % (self.root, os.sep, suffix)

    def _create_config_dict(self):
        """
        Create a dictionary with the trogdor_agent configuration.

        :return:            The configuration dictionary.
        """
        dict_nodes = {}
        for node in self.nodes:
            dict_nodes[node.name()] = {
                "hostname": node.hostname(),
                "agent_port": self.port,
            }
        return {
            "platform": "basic",
            "log": {
                "path": self.log_path,
            },
            "nodes": dict_nodes
        }

    def start_node(self, node):
        # Create the configuration file on the node.
        str = json.dumps(self._create_config_dict())
        self.logger.info("Creating configuration file %s with %s" % (self.config_path, str))
        node.account.create_file(self.config_path, str)

        # Start the trogdor_agent process on the node.
        node.account.ssh("trogdor_agent --config %s --name %s", (self.config_path, node.name()))

    def wait_node(self, node, timeout_sec=None):
        return node.account.ssh("ps -o pid= -C trogdor_agent") == 0

    def stop_node(self, node):
        """Halt trogdor_agent process(es) on this node."""
        node.account.ssh("PIDS=$(ps -o pid= -C trogdor_agent); [[ -z $PIDS ]] || kill ${PIDS}")

    def clean_node(self, node):
        """Clean up persistent state on this node - e.g. service logs, configuration files etc."""
        self.stop_node(node)
        node.account.ssh("rm -f %s %s" % (self.log_path, self.config_path))
