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
    def __init__(self, agent):
        self.agent = agent

    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()

    def do_GET(s):
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()
        s.wfile.write("<html><head><title>Title goes here.</title></head>")
        s.wfile.write("<body><p>This is a test.</p>")
        s.wfile.write("<p>You accessed path: %s</p>" % s.path)
        s.wfile.write("</body></html>")


class Agent(object):
    def __init__(self, plat, opts):
        self.plat = plat
        self.opts = opts
        self.double_log("Launching trogdor agent %d with options: %s and platform: %s" %
                        (os.getpid(), opts, plat.name()))

    def double_log(self, msg):
        """ Log to both stdout and the platform log. """
        print(msg)
        self.plat.log.info(msg)

    def run(self):
        """ Run the Trogdor agent. """
        self.httpd = BaseHTTPServer.HTTPServer(server_address=('', self.opts.port),
                                               RequestHandlerClass=AgentHttpHandler)
        try:
            self.httpd.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            self.httpd.server_close()
        self.double_log("Stopping trogdor agent")


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
