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
import json
import sys
import traceback


class LoggedHttpHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    """
    A base class for HTTP handlers which handles logging some events and
    errors.

    Note: you must set server.log to your platform.Log object.
    """

    def handle_get(self):
        raise NotImplemented

    def handle_put(self):
        raise NotImplemented

    def do_HEAD(self):
        self.do_GET()

    def do_GET(self):
        try:
            self.handle_get()
        except Exception as e:
            self.send_error_response(e)

    def do_PUT(self):
        try:
            self.handle_put()
        except Exception as e:
            self.send_error_response(e)

    def send_success_response(self, status, str):
        self.server.log.trace("HTTP %s %s: status %d: %s" %
                             (self.command, self.path, status, str.strip('\n')))
        self._send_response(status, str)

    def send_error_response(self, e):
        _, _, tb = sys.exc_info()
        err = str(e) + "\n" + "".join(traceback.format_tb(tb))
        self.server.log.warn("HTTP %s %s: status %d: %s" % (self.command, self.path, 400, err))
        values = { 'error': str(e) }
        self._send_response(400, json.dumps(values))

    def _send_response(self, status, str):
        try:
            self.send_response(status)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(str)
        except Exception as e:
            self.server.log.warn("Error sending response to %s %s: %s" %
                                (self.command, self.path, traceback.format_exc()))
