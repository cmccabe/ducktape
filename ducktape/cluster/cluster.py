# Copyright 2014 Confluent Inc.
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

import collections


class ClusterNode(object):
    """
    Represents a node in a Ducktape cluster.

    Nodes have names, which may or may not be the same as their hostnames and account
    objects which can be used to contact them.
    """
    def __init__(self, name, account):
        """
        Create a new ClusterNode

        :param name:            A string which uniquely identifies this node.
        :param account:         The account to use for this node.
        """
        self.name = name
        self.account = account

    @property
    def logger(self):
        return self.account.logger

    @property
    def hostname(self):
        return self.account.hostname


class Cluster(object):
    """ Interface for a cluster -- a collection of nodes with login credentials.
    This interface doesn't define any mapping of roles/services to nodes. It only interacts with some underlying
    system that can describe available resources and mediates reservations of those resources. This is intentionally
    simple right now: the only "resource" right now is a generic VM and it is assumed everything is approximately
    homogeneous.
    """

    def __len__(self):
        """Size of this cluster object. I.e. number of 'nodes' in the cluster."""
        raise NotImplementedError()

    def alloc(self, num_nodes):
        """Try to allocate the specified number of ClusterNode objects.

        :param num_nodes:       The number of nodes to allocate.
        """
        raise NotImplementedError()

    def request(self, num_nodes):
        """Identical to alloc. Keeping for compatibility"""
        return self.alloc(num_nodes)

    def num_available_nodes(self):
        """Number of available nodes."""
        raise NotImplementedError()

    def free(self, nodes):
        """Free the given node or list of nodes"""
        if isinstance(nodes, collections.Iterable):
            for s in nodes:
                self.free_single(s)
        else:
            self.free_single(nodes)

    def free_single(self, node):
        raise NotImplementedError()

    def __eq__(self, other):
        return other is not None and self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(tuple(sorted(self.__dict__.items())))
