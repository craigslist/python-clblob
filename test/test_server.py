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

'''Tests for craigslist blob server module.'''

import httplib
import json
import time

import test

HOST = test.CONFIG['clblob']['client']['replicas']['000']['ip']
PORT = test.CONFIG['clblob']['client']['replicas']['000']['port']


def request(method, url, *args, **kwargs):
    '''Perform the request and handle the response.'''
    connection = httplib.HTTPConnection(HOST, PORT)
    connection.request(method, url, *args, **kwargs)
    return connection.getresponse()


class TestServer(test.Base):

    def test_blob(self):
        response = request('PUT', '/test1', 'test data')
        self.assertEquals(200, response.status)
        info = json.loads(response.read())

        time.sleep(0.2)
        response = request('GET', '/%s' % info['name'])
        self.assertEquals(200, response.status)
        self.assertEquals('application/octet-stream',
            response.getheader('Content-Type'))
        self.assertEquals('test data', response.read())

        response = request('GET', '/%s?response=info' % info['name'])
        self.assertEquals(200, response.status)
        self.assertEquals('application/json',
            response.getheader('Content-Type'))
        self.assertEquals(info, json.loads(response.read()))

        response = request('DELETE', '/%s' % info['name'])
        self.assertEquals(200, response.status)
        self.assertEquals('application/json',
            response.getheader('Content-Type'))
        info2 = json.loads(response.read())
        self.assertEquals(info.pop('modified'), info2.pop('modified'))
        self.assertNotEquals(info.pop('deleted'), info2.pop('deleted'))
        self.assertNotEquals(info.pop('modified_deleted'),
            info2.pop('modified_deleted'))
        self.assertEquals(info, info2)

    def test_not_found(self):
        response = request('GET', '/00000_test2')
        self.assertEquals(404, response.status)

    def test_bad_method(self):
        response = request('POST', '/test')
        self.assertEquals(405, response.status)

    def test_configcheck(self):
        config = '{"clblob":{"client":{"clusters":[],"replicas":{}}}}'
        response = request('PUT', '/_configcheck', config)
        self.assertEquals(200, response.status)
        self.assertEquals('application/json',
            response.getheader('Content-Type'))
        response = request('PUT',
            '/_configcheck/001?brief=true&tolerance=catastrophic', config)
        self.assertEquals(200, response.status)
        self.assertEquals('application/octet-stream',
            response.getheader('Content-Type'))
        self.assertEquals('OK', response.read())

    def test_config(self):
        response = request('GET', '/_config')
        self.assertEquals(200, response.status)
        self.assertEquals('application/json',
            response.getheader('Content-Type'))
        self.assertEquals(json.loads(response.read()),
            self.servers['000'].config)

    def test_console(self):
        response = request('GET', '/_console')
        self.assertEquals(200, response.status)
        self.assertEquals('text/html', response.getheader('Content-Type'))

    def test_admin(self):
        for command in ['buffer', 'list', 'purge', 'status', 'sync']:
            response = request('GET', '/_%s' % command)
            self.assertEquals(200, response.status)
            self.assertEquals('application/json',
                response.getheader('Content-Type'))
            response = request('GET', '/_%s/001' % command)
            self.assertEquals(200, response.status)
            self.assertEquals('application/json',
                response.getheader('Content-Type'))

    def test_admin_bad(self):
        response = request('GET', '/_bad')
        self.assertEquals(404, response.status)
        response = request('GET', '/_sync/bad')
        self.assertEquals(500, response.status)
