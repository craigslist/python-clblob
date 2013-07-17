# Copyright 2013 craigslist
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''craigslist blob server module.

This module creates a thin HTTP wrapper over the client module. The server
exists to provide a persistent client for incoming HTTP requests. These
requests are calls to query or manipulate blobs or the blob service
itself. Each server instance is bound to a client library using a **shared
client object**.

When the server receives an incoming **request**, it calls the appropriate
method on shared client instance. When the client returns with the result,
the server returns this to the recipient as the body of the HTTP response.

Since the server is bound to the client with a shared client object,
and this object stores call state, the server can process incoming
requests independently (and asynchronously) from the client's processing
of requests.

Beyond the blob and admin methods of the client instance, the server
also supportes retreiving the config object for this server, as well
as a console page. The html diagnostic and admin console lives in
**console.html**. It is simply a web page frontend that uses AJAX requests
to query status information for each replica using the same HTTP calls that
the server/client system uses to manage the blob service. After starting
the server visit /_console on the server host and port to view it.'''

import json
import os.path
import mimetypes

import clblob.client
import clcommon.config
import clcommon.http
import clcommon.server

DEFAULT_CONFIG = clcommon.config.update(clblob.client.DEFAULT_CONFIG,
    clcommon.http.DEFAULT_CONFIG)

DEFAULT_CONFIG_FILES = clblob.client.DEFAULT_CONFIG_FILES + [
    '/etc/clblobserver.conf',
    '~/.clblobserver.conf']

DEFAULT_CONFIG_DIRS = clblob.client.DEFAULT_CONFIG_DIRS + [
    '/etc/clblobserver.d',
    '~/.clblobserver.d']


class Request(clcommon.http.Request):
    '''Request handler for blobs.'''

    def run(self):
        '''Run the request.'''
        name = self.env['PATH_INFO'].strip('/')
        if name[:1] == '_':
            name, body = self._run_admin(name[1:])
        else:
            name, body = self._run(name)
        if hasattr(body, 'read') or isinstance(body, basestring):
            content_type = mimetypes.guess_type(name)
            if content_type[0] is None:
                content_type = 'application/octet-stream'
            else:
                content_type = content_type[0]
            self.headers.append(('Content-type', content_type))
            if hasattr(body, 'content_length'):
                self.headers.append(('Content-Length', body.content_length))
        else:
            body = json.dumps(body, indent=4, sort_keys=True)
            self.headers.append(('Content-type', 'application/json'))
        return self.ok(body)

    def _run(self, name):
        '''Run a command.'''
        kwargs = self.parse_params(['replicate', 'response'],
            ['deleted', 'modified', 'modified_deleted', 'ttl'], ['encoded'])
        if self.method == 'DELETE':
            body = self.server.blob_client.delete(name, **kwargs)
        elif self.method == 'GET':
            try:
                body = self.server.blob_client.get(name, **kwargs)
            except clblob.InvalidRequest, exception:
                self.log.warning(_('Invalid request: %s'), exception)
                raise clcommon.http.BadRequest()
            except clblob.NotFound:
                raise clcommon.http.NotFound()
        elif self.method == 'PUT':
            body = self.server.blob_client.put(name, self.body, **kwargs)
        else:
            raise clcommon.http.MethodNotAllowed()
        return name, body

    def _run_admin(self, name):
        '''Run an admin command.'''
        parts = name.split('/', 1)
        if len(parts) == 1:
            parts.append(None)
        if name == 'config':
            body = self.server.config
        elif name == 'console':
            name = os.path.join(os.path.dirname(__file__), 'console.html')
            body = open(name)
        elif name == 'jquery.js':
            body = open(clcommon.http.JQUERY)
        elif name == 'favicon.ico':
            body = open(clcommon.http.FAVICON)
        elif parts[0] == 'buffer':
            body = self.server.blob_client.buffer(parts[1])
        elif parts[0] == 'configcheck':
            if len(self.body_data) == 0:
                raise clcommon.http.BadRequest()
            kwargs = self.parse_params(['tolerance'], None, ['brief'])
            body = self.server.blob_client.configcheck(self.body_data,
                parts[1], **kwargs)
        elif parts[0] == 'list':
            kwargs = self.parse_params(None,
                ['modified_start', 'modified_stop', 'checksum_modulo'],
                ['checksum'])
            body = self.server.blob_client.list(parts[1], **kwargs)
        elif parts[0] == 'purge':
            body = self.server.blob_client.purge(parts[1])
        elif parts[0] == 'status':
            body = self.server.blob_client.status(parts[1])
        elif parts[0] == 'sync':
            kwargs = self.parse_params(['source'],
                ['modified_start', 'modified_stop'])
            body = self.server.blob_client.sync(parts[1], **kwargs)
        else:
            raise clcommon.http.NotFound()
        return name, body


class Server(clcommon.http.Server):
    '''Wrapper for the HTTP server that adds a blob client that will be used
    for all request handling.'''

    def __init__(self, config, request):
        super(Server, self).__init__(config, request)
        self.blob_client = None

    def start(self):
        self.blob_client = clblob.client.Client(self.config)
        super(Server, self).start()

    def stop(self, timeout=None):
        super(Server, self).stop(timeout)
        self.blob_client.stop()
        self.blob_client = None


if __name__ == '__main__':
    clcommon.server.Server(DEFAULT_CONFIG, DEFAULT_CONFIG_FILES,
        DEFAULT_CONFIG_DIRS, [lambda config: Server(config, Request)]).start()
