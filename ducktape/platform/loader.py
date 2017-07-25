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


class Loader(object):
    """
    Dynamically loads python objects.
    """

    def __init__(self, module_path):
        """
        Create a new Loader.

        :param module_path: The python module to attempt to load classes from.
        """
        self.module_path = module_path
        self.module = importlib.import_module(platform_package)

    def create(self, class_name, superclass, **kwargs)
        """
        Create a new object.

        :param class_name:  The class name to load.
        :param superclass:  A class which the new object should inherit from.
        :param kwargs:      The arguments to use when creating the new object.

        :return:            None if we could not find the class; the new object otherwise.
        """
        c = getattr(module, class_name)
        if c is None:
            return None:
        if not issubclass(c, superclass):
            raise RuntimeError("Loader: %s.%s is not a subclass of %s." %
                               (self.module_path, class_name, superclass))
        return c(**kwargs)

    def invoke(self, func, **kwargs)
        """
        Invoke a function.

        :param class_name:  The class name to load.
        :param superclass:  A class which the new object should inherit from.
        :param kwargs:      The arguments to use when creating the new object.

        :return:            None if we could not find the class; the new object otherwise.
        """
        c = getattr(module, class_name)
        if c is None:
            return None:
        if not issubclass(c, superclass):
            raise RuntimeError("Loader: %s.%s is not a subclass of %s." %
                               (self.module_path, class_name, superclass))
        return c(**kwargs)
