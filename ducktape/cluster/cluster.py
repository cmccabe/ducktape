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

from ducktape.cluster.cluster_spec import ClusterSpec


class ClusterNode(object):
    def __init__(self, account, **kwargs):
        self.account = account
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def name(self):
        return self.account.host

    @property
    def operating_system(self):
        return self.account.operating_system


class Cluster(object):
    """ Interface for a cluster -- a collection of nodes with login credentials.
    This interface doesn't define any mapping of roles/services to nodes. It only interacts with some underlying
    system that can describe available resources and mediates reservations of those resources.
    """

    def __len__(self):
        """Size of this cluster object. I.e. number of 'nodes' in the cluster."""
        return self.available().size() + self.used().size()

    def alloc(self, cluster_spec_or_num_nodes):
        """
        Allocate some nodes.

        :param cluster_spec_or_num_nodes:       Either a cluster_spec or a number of basic Linux
                                                nodes to allocate.
        :throws InsufficientResources:          If the nodes cannot be allocated.
        """
        if isinstance(cluster_spec_or_num_nodes, (int, long)):
            return self.alloc_spec(ClusterSpec.basic_linux(cluster_spec_or_num_nodes))
        else:
            return self.alloc_spec(cluster_spec_or_num_nodes)

    def request(self, cluster_spec_or_num_nodes):
        """
        Deprecated compatibility wrapper for alloc.

        :param cluster_spec_or_num_nodes:       Either a cluster_spec or a number of basic Linux
                                                nodes to allocate.
        :throws InsufficientResources:          If the nodes cannot be allocated.
        """
        return self.alloc(cluster_spec_or_num_nodes)

    def alloc_spec(self, cluster_spec):
        """
        Allocate some nodes.  Suclasses should implement this method.

        :param cluster_spec:                    The specification of the nodes to allocate.
        :throws InsufficientResources:          If the nodes cannot be allocated.
        """
        raise NotImplementedError

    def free_all(self):
        """Free all nodes which are in use."""
        self.free(self.used())

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

    def available(self):
        """
        Return a ClusterSpec object describing the currently available nodes.
        """
        raise NotImplementedError

    def used(self):
        """
        Return a ClusterSpec object describing the currently in use nodes.
        """
        raise NotImplementedError

    def all(self):
        """
        Return a ClusterSpec object describing all nodes.
        """
        return self.available().clone().add(self.used())
