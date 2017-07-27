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
import pkgutil
from types import FunctionType, TypeType


class Loader(object):
    """
    Dynamically loads python objects from packages.
    """

    def __init__(self, package_name):
        """
        Create a new Loader.

        :param module_path: The python package to attempt to load classes from.
        """
        self.package_name = package_name

    def create(self, class_name, superclass, **kwargs):
        """
        Create a new object.

        :param class_name:  The class name to load.
        :param superclass:  A class which the new object should inherit from.
        :param kwargs:      The arguments to use when creating the new object.

        :return:            None if we could not find the class; the new object otherwise.
        """
        loader = pkgutil.get_loader(self.package_name)
        for module_loader, name, is_package in pkgutil.walk_packages([loader.filename]):
            full_name = self.package_name + "." + name
            module = importlib.import_module(full_name)
            if hasattr(module, class_name):
                element = getattr(module, class_name)
                if type(element) == TypeType:
                    return element(**kwargs)
        return None

    def invoke(self, module_name, function_name, **kwargs):
        """
        Invoke a function.

        :param module_name:     The module name.
        :param function_name:   The function name to invoke.
        :param kwargs:          The arguments to use when invoking the function.

        :return:                None if we could not find the function; the function
                                result otherwise.
        """
        loader = pkgutil.get_loader(self.package_name)
        for module_loader, name, is_package in pkgutil.walk_packages([loader.filename]):
            if name == module_name:
                full_name = self.package_name + "." + name
                module = importlib.import_module(full_name)
                if hasattr(module, function_name):
                    element = getattr(module, function_name)
                    if type(element) == FunctionType:
                        return element(**kwargs)
        return None
