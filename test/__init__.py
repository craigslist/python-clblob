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

'''Tests for craigslist blob package.'''

# The worker module must be imported before any monkey patching happens.
import clcommon.worker
import gevent.monkey
gevent.monkey.patch_all()

import os.path
import shutil
import time
import unittest

import clblob.client
import clblob.server
import clcommon.config
import clcommon.http

CONFIG = clcommon.config.update(clblob.client.DEFAULT_CONFIG, {
    'clblob': {
        'client': {
            'buffer_interval': 1,
            'buffer_start_delay': 0,
            'checksum_modulo': 100,
            'cluster': 0,
            'cluster_event_buffer_size': 10,
            'clusters': [
                [
                    {'replicas': ['000', '001', '002'], 'write_weight': 1},
                    {'replicas': ['010', '011', '012'], 'write_weight': 1}
                ],
                [
                    {'replicas': ['100', '101', '102'], 'write_weight': 1},
                    {'replicas': ['110', '111', '112'], 'write_weight': 1},
                    {'replicas': ['120', '121', '122'], 'write_weight': 1}
                ]
            ],
            'expiration_delay': 100,
            'purge_interval': 1,
            'purge_start_delay': 0,
            'replica_event_buffer_size': 10,
            'replicas': {
                '000': {'ip': '127.0.0.1', 'port': 20000, 'read_weight': 1},
                '001': {'ip': '127.0.0.1', 'port': 20001, 'read_weight': 1},
                '002': {'ip': '127.0.0.1', 'port': 20002, 'read_weight': 1},
                '010': {'ip': '127.0.0.1', 'port': 20010, 'read_weight': 1},
                '011': {'ip': '127.0.0.1', 'port': 20011, 'read_weight': 1},
                '012': {'ip': '127.0.0.1', 'port': 20012, 'read_weight': 1},
                '100': {'ip': '127.0.0.1', 'port': 20100, 'read_weight': 1},
                '101': {'ip': '127.0.0.1', 'port': 20101, 'read_weight': 1},
                '102': {'ip': '127.0.0.1', 'port': 20102, 'read_weight': 1},
                '110': {'ip': '127.0.0.1', 'port': 20110, 'read_weight': 1},
                '111': {'ip': '127.0.0.1', 'port': 20111, 'read_weight': 1},
                '112': {'ip': '127.0.0.1', 'port': 20112, 'read_weight': 1},
                '120': {'ip': '127.0.0.1', 'port': 20120, 'read_weight': 1},
                '121': {'ip': '127.0.0.1', 'port': 20121, 'read_weight': 1},
                '122': {'ip': '127.0.0.1', 'port': 20122, 'read_weight': 1}},
            'sync_blob_threshold': 5,
            'sync_checksum_modulo_factor': 5},
        'index': {'sqlite': {'sync': 0}},
        'store': {'disk': {'sync': False}}}})


class Base(unittest.TestCase):
    '''Base test class for blob services that handles test cluster setup.'''

    config = CONFIG

    def __init__(self, *args, **kwargs):
        super(Base, self).__init__(*args, **kwargs)
        self.servers = {}

    def setUp(self):
        '''Start a bunch of servers for blob testing.'''
        config = clcommon.config.update(clblob.server.DEFAULT_CONFIG,
            self.config)
        shutil.rmtree('test_blob', ignore_errors=True)
        for name, value in config['clblob']['client']['replicas'].iteritems():
            path = os.path.join('test_blob', name)
            os.makedirs(path)
            config = clcommon.config.update(config, {
                'clblob': {
                    'client': {'replica': name},
                    'index': {
                        'sqlite': {
                            'database': os.path.join(path, '_index'),
                            'tmpdir': os.path.join(path, '_index_tmp')}},
                    'store': {'disk': {'path': path}}},
                'clcommon': {
                    'http': {
                        'host': value['ip'],
                        'port': value['port']}}})
            server = clblob.server.Server(config, clblob.server.Request)
            server.start()
            self.servers[name] = server

    def tearDown(self):
        time.sleep(0.2)
        for name in list(self.servers):
            self.servers[name].stop()
            del self.servers[name]
