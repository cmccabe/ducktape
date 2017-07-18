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


from ducktape.platform.log import Log


def default_platform():
    return "ducktape.platform.basic.basic_platform"
    #from ducktape.platform.basic.basic_platform import BasicPlatform
    #return str(BasicPlatform).split("'")[1]


def create_platform(platform_package, config_file):
    """ Create a platform object from a package name. """
    print "WATERMELON: attempting to import " + platform_package
    module = importlib.import_module(platform_package)
    return module.create_platform(config_file)


class Platform(object):
    """ The platform we are running on. """
    def __init__(self):
        self.log = Log()

    def name(self):
        """ Return the platform name. """
        raise NotImplementedError()

#    def log(self):
#        """ Return a log object for this platform. """
#        raise NotImplementedError()
