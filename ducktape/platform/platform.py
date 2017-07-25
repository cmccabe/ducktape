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
    module_paths = data.get("modules")
    if module_paths is None:
        module_paths = [ "ducktape.platform.fault.fault_spec",
                         "ducktape.basic_platform.basic_platform" ]
    loaders = [ Loader(module_path) for module_path in module_paths ]
    platform_package = data.get("platform")
    if platform_package == None:
        platform_package = "BasicPlatform"
    for loader in loaders:
        platform = loader.create(name, class_name, FaultSpec.__class__, dict)
        if platform is not None:
            return platform
    loader_paths = [ loader.module_path for loader in loaders ]
    raise RuntimeError("Failed to resolve platform type '%s' in %s" %
                       (class_name, ",".join(loader_paths)))


class Platform(object):
    """ The platform we are running on. """
    def __init__(self, name, log, topology, loaders):
        """
        Initialize the platform object.
        :param name:        A string identifying the platform.
        :param log:         A platform.Log object.
        :param topology:    A platform.Topology object.
        :param loaders:     A list of platform.Loader objects for loading classes.
        """
        self.name = name
        self.log = log
        self.topology = topology
        self.loaders = loaders

    def create_fault_spec_from_json(self, text):
        """
        Create a new fault specification object from a JSON string.

        :param text:        The JSON string
        :return:            The new fault specification object.
        """
        return self.create_fault_spec_from_dict(json.loads(text))

    def create_fault_spec_from_dict(self, dict):
        """
        Create a new fault specification object from a dictionary.

        :param dict:        The dictionary.
        :return:            The new fault specification object.
        """
        kind = dict.get("kind")
        if kind is None:
            raise RuntimeError("The fault specification does not include a 'kind'.")
        class_name = "%sSpec" % kind
        for loader in self.loaders:
            fault_spec = loader.create(name, class_name, FaultSpec.__class__, dict)
            if fault_spec is not None:
                return fault_spec
        loader_paths = [ loader.module_path for loader in self.loaders ]
        raise RuntimeError("Failed to resolve fault spec type '%s' in %s" %
                           (class_name, ",".join(loader_paths)))

    def create_fault(self, name, start_ms, duration_ms, spec):
        """
        Create a new fault object.  This does not activate the fault.

        :param name:        The fault name.
        :param start_ms:    The start time in milliseconds.
        :param duration_ms: The duration in milliseconds.
        :param spec:        The fault spec object.
        :return:            The new fault object.
        """
        class_name = spec.kind
        for loader in self.loaders:
            fault = loader.create(name, class_name, Fault.__class__,
                                  start_ms=start_ms, duration_ms=duration_ms, spec=spec)
            if fault is not None:
                return fault
        loader_paths = [ loader.module_path for loader in self.loaders ]
        raise RuntimeError("Failed to resolve fault type '%s' in %s" %
                           (class_name, ",".join(loader_paths)))

    def __str__(self):
        return self.name
