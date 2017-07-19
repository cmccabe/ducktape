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

import BaseHTTPServer
import argparse
import os
import sys

from datetime import datetime
from dateutil.tz import tzlocal
from ducktape.platform.platform import create_platform, default_platform


class AgentOptions(object):
    """ The configuration passed to the agent on the command line. """
    def __init__(self, platform_package, port, config_file):
        self.platform_package = platform_package
        self.port = port
        self.config_file = config_file

    def __str__(self):
        return '{"platform_package": %s, "port": %d, "config_file": "%s"}' % \
               (self.platform_package, self.port, self.config_file)


class AgentHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    agent = None

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        p = self.path.strip('/')
        if p == "status":
           self.send_str_response(200, self.agent.get_status())
        else:
            self.send_str_response(404, "Unknown path %s\n" % self.path)

    def do_PUT(self):
        p = self.path.strip('/')
        if p == "shutdown":
            self.send_str_response(200, '{"status": "success"}\n')
            self.agent.shutdown()
        else:
            self.send_str_response(404, "Unknown path %s\n" % self.path)

    def send_str_response(self, status, str):
        self.send_response(status)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(str)

class Agent(object):
    def __init__(self, plat, opts):
        self.plat = plat
        self.log = plat.log
        self.opts = opts
        self.double_log("Launching trogdor agent %d with options: %s and platform: %s" %
                        (os.getpid(), opts, plat.name()))

    def double_log(self, msg):
        """ Log to both stdout and the platform log. """
        print(msg)
        self.plat.log.info(msg)

    def run(self):
        """ Run the Trogdor agent. """
        self.start_time = datetime.now(tzlocal())
        AgentHttpHandler.agent = self
        self.httpd = BaseHTTPServer.HTTPServer(server_address=('', self.opts.port),
                                               RequestHandlerClass=AgentHttpHandler)
        try:
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.httpd.server_close()
        self.double_log("Stopping trogdor agent")

    def shutdown(self):
        self.log.info("Shutting down %d by request." % os.getpid())
        os._exit(0)

    def get_status(self):
        start_time_str = "{:%FT%T%z}".format(self.start_time)
        return '{"started": "%s"}\n' % start_time_str


def main():
    print "default platform = " + default_platform()
    parser = argparse.ArgumentParser(description=
        "The agent process for the Trogdor fault injection system.")
    parser.add_argument("--port", action="store", type=int, default=8888,
        help="The control port to use.")
    parser.add_argument("--platform", action="store", default=default_platform(),
                        dest="platform_package", help="The platform class to use.")
    parser.add_argument("--config_file", action="store", dest="config_file",
                        required=True, help="The platform configuration file.")
    args = vars(parser.parse_args(sys.argv[1:]))
    opts = AgentOptions(args.get("platform_package"), args.get("port"), args.get("config_file"))
    plat = create_platform(opts.platform_package, opts.config_file)
    agent = Agent(plat, opts)
    agent.run()
