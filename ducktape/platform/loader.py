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
from types import ModuleType, MethodType, FunctionType, ClassType, TypeType


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
        self.package = importlib.import_module(package_name)
        print "WATERMELON type(package) = %s" % type(self.package)

    def create(self, class_name, superclass, **kwargs):
        """
        Create a new object.

        :param class_name:  The class name to load.
        :param superclass:  A class which the new object should inherit from.
        :param kwargs:      The arguments to use when creating the new object.

        :return:            None if we could not find the class; the new object otherwise.
        """
        loader = pkgutil.get_loader(self.package_name)
        print "WATERMELON2: package_name=%s, loader.filename=%s" % (self.package_name, loader.filename)
        for module_loader, name, is_package in pkgutil.walk_packages([loader.filename]):
            full_name = self.package_name + "." + name
            module = importlib.import_module(full_name)
            #print "WATERMELON3: name=%s, self.package_name=%s, full_name=%s" % (name, self.package_name, full_name)
            if hasattr(module, class_name):
                print "WATERMELON4: name=%s, self.package_name=%s, full_name=%s" % (name, self.package_name, full_name)
                element = getattr(module, class_name)
                print "WATERMELON5: type(element) = %s" % type(element)
                if type(element) == TypeType:
                    print "WATERMELON5: name=%s, self.package_name=%s, full_name=%s" % (name, self.package_name, full_name)
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
        print "WATERMELON2: package_name=%s, loader.filename=%s" % (self.package_name, loader.filename)
        for module_loader, name, is_package in pkgutil.walk_packages([loader.filename]):
            if name == module_name:
                full_name = self.package_name + "." + name
                module = importlib.import_module(full_name)
                #print "WATERMELON3: name=%s, self.package_name=%s, full_name=%s" % (name, self.package_name, full_name)
                if hasattr(module, function_name):
                    print "WATERMELON4: name=%s, self.package_name=%s, full_name=%s" % (name, self.package_name, full_name)
                    element = getattr(module, function_name)
                    print "WATERMELON5: type(element) = %s" % type(element)
                    if type(element) == FunctionType:
                        print "WATERMELON5: name=%s, self.package_name=%s, full_name=%s" % (name, self.package_name, full_name)
                        return element(**kwargs)
        return None
#            for element_name in dir(module):
#                element = getattr(module, element_name)
#                print "WATERMELON4: full_name=%s, element_name=%s, type(element)=%s" % (full_name, element_name, type(element))
            #for element in dir(mymodule):
            #print("WATERMELON3: %s" % qname)


#        print "WATERMELON Loader(package_name=%s) invoke" % self.package_name
#        def _trim_leading_components(s):
#            period_idx = s.rfind(".")
#            return s[period_idx+1:]
#
#        for m in dir(self.package):
#            module = getattr(self.package, m)
#            if not type(module) == ModuleType:
#                print "found non-module %s.  type == %s" % (module, type(module))
#            if type(module) == ModuleType:
#                print "found module %s.  module.__name__ = %s" % (module, _trim_leading_components(module.__name__))
#                if module.__name__ == module_name:
#                    for c in dir(module):
#                        f = getattr(module, c)
#                        if type(f) == MethodType:
#                            if f.__name__ == function_name:
#                                return f(**kwargs)

