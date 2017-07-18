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

import ConfigParser
from ducktape.platform.basic.basic_log import BasicLog
from ducktape.platform.platform import Platform


def create_platform(config_file):
    config = BasicPlatformConfig(config_file)
    return BasicPlatform(config)

#externally_routable_ip
#ssh_config
#host
#hostname
#identityfile
#password
#port
#user

class BasicPlatformConfig(object):
    def __init__(self, config_file):
        parser = ConfigParser.ConfigParser()
        with open(config_file, 'r') as input_file:
            parser.readfp(input_file)
        self.log_path = parser.get("log", "path")


class BasicPlatform(Platform):
    """
    Implements the basic platform.

    In this platform, we assume:
    * we can ssh into nodes based on their names.
    * we can invoke iptables to create network partitions
    """

    def __init__(self, config):
        super(BasicPlatform, self).__init__()
        self.log = BasicLog(config.log_path)

    def name(self):
        return "BasicPlatform"
