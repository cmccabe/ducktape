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

from ducktape.platform.fault.fault_spec import FaultSpec
from ducktape.platform.loader import Loader


def create_platform(config_path):
    """ Create a platform object from a package name. """
    with open(config_path) as fp:
        data = json.load(fp)
    module_paths = data.get("modules")
    if module_paths is None:
        module_paths = [ "ducktape.platform.fault", "ducktape.basic_platform" ]
    loaders = [ Loader(module_path) for module_path in module_paths ]
    platform_module = data.get("platform")
    if platform_module == None:
        platform_module = "basic_platform"
    for loader in loaders:
        platform = loader.invoke(platform_module, "create_platform", config_path=config_path, loaders=loaders)
        if platform is not None:
            return platform
    loader_packages = [ loader.package_name for loader in loaders ]
    raise RuntimeError("Failed to find platform type '%s' in %s" %
                       (platform_module, ", ".join(loader_packages)))


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
            fault_spec = loader.create(class_name, FaultSpec, args=dict)
            if fault_spec is not None:
                return fault_spec
        loader_packages = [ loader.package_name for loader in self.loaders ]
        raise RuntimeError("Failed to resolve fault spec type '%s' in %s" %
                           (class_name, ",".join(loader_packages)))

    def create_fault(self, name, spec):
        """
        Create a new fault object.  This does not activate the fault.

        :param name:        The fault name.
        :param spec:        The fault spec object.
        :return:            The new fault object.
        """
        return spec.to_fault(name, self.log, self.loaders)

    def __str__(self):
        return self.name
