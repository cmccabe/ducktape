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

import argparse


def parse_options(args):
    """ Create an Options object from an array of command-line arguments. """


class Options(object):
    MAX_PORT = 65535

    """ The options for running the Trogdor agent process. """
    def __init__(self, port):
        self.port = port
        if (self.port <= 0):
            raise RuntimeError("Invalid port %d. Port cannot be less than or equal to 0." % port)
        if (self.port >= Options.MAX_PORT):
            raise RuntimeError("Invalid port %d. Port cannot be greater than %s." % (port, Options.MAX_PORT))

    def __str__(self):
        return ("Options{\"port\": %d}" % self.port)
