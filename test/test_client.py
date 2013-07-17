# -*- coding: utf-8 -*-
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

'''Tests for craigslist blob client module.'''

import hashlib
import httplib
import os.path
import shutil
import socket
import sqlite3
import StringIO
import time

import clblob.client
import clcommon.config
import test

# Vars for blob used in some of the tests.
NAME = 'test1'
ENCODED = '00100_%s' % NAME
PATH = 'test_blob/%%s/77/9b/%s' % ENCODED
REPLICAS0 = ['010', '011', '012']
REPLICAS1 = ['100', '101', '102']
REPLICAS = REPLICAS0 + REPLICAS1
OTHERS = ['000', '001', '002', '110', '111', '112', '120', '121', '122']


class TestClient(test.Base):

    def test_config(self):
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            clblob.client.DEFAULT_CONFIG)

        config = clcommon.config.update_option(clblob.client.DEFAULT_CONFIG,
            'clblob.client.clusters', [[]])
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)

        clblob.client.Client(self.config)

        config = clcommon.config.update_option(self.config,
            'clblob.client.cluster', 0)
        clblob.client.Client(config)

        config = clcommon.config.update_option(self.config,
            'clblob.client.cluster', 10)
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)

    def test_config_replica(self):
        config = clcommon.config.update_option(self.config,
            'clblob.client.replica', 'bad')
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)

        config = clcommon.config.update_option(self.config,
            'clblob.client.replica', '"000"')
        config = clcommon.config.update_option(config,
            'clblob.client.clusters', [[
                {'replicas': ['000', '001', '002'], 'write_weight': 1},
                {'replicas': ['000', '001', '002'], 'write_weight': 1}]])
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)

        config = clcommon.config.update_option(config,
            'clblob.client.clusters',
            [[{'replicas': ['bad'], 'write_weight': 1}]])
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)

        config = clcommon.config.update_option(config,
            'clblob.client.clusters', [[{'replicas': ['000', '001', '002']}]])
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)

        self.assertEquals('000', open('test_blob/000/_replica').read())
        self.assertEquals('101', open('test_blob/101/_replica').read())

    def test_config_path(self):
        replica_config = clcommon.config.update_option(self.config,
            'clblob.client.replica', '"000"')
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            replica_config)

        config = clcommon.config.update_option(replica_config,
            'clblob.store.disk.path', 'test_blob')
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            replica_config)

        config = clcommon.config.update_option(replica_config,
            'clblob.index.sqlite.database', 'test_blob/_index')
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            replica_config)

        config = clcommon.config.update_option(replica_config,
            'clblob.store.disk.path', 'test_blob')
        config = clcommon.config.update_option(config,
            'clblob.index.sqlite.database', 'test_blob/_index')
        clblob.client.Client(config)

        config = clcommon.config.update_option(config, 'clblob.client.replica',
            '"001"')
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)

        shutil.rmtree('test_blob/bad', ignore_errors=True)
        open('test_blob/bad', 'w').close()
        config = clcommon.config.update_option(replica_config,
            'clblob.store.disk.path', 'test_blob/bad')
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)

        if not os.path.exists('test_blob_perms'):
            os.makedirs('test_blob_perms')
        os.chmod('test_blob_perms', 0000)
        config = clcommon.config.update_option(replica_config,
            'clblob.store.disk.path', 'test_blob_perms')
        self.assertRaises(clcommon.config.ConfigError, clblob.client.Client,
            config)
        os.chmod('test_blob_perms', 0755)

    def test_old_replica(self):
        self.servers['000'].stop()
        index = sqlite3.connect('test_blob/000/_index')
        index.execute('INSERT INTO replica VALUES("old")')
        index.commit()
        index.close()
        self.servers['000'].start()

    def test_corrupt_index(self):
        server = self.servers.pop('000')
        server.stop()
        open('test_blob/000/_index', 'w').write('garbage')
        self.assertRaises(sqlite3.DatabaseError, server.start)

    def test_buckets(self):
        client = clblob.client.Client(self.config)
        self.assertEquals({0: 1, 1: 0}, client.buckets(NAME))
        self.assertEquals({0: 1, 1: 0}, client.buckets(ENCODED, True))

    def test_delete(self):
        client = clblob.client.Client(self.config)
        info = client.put(NAME, 'delete me')
        self.assertEquals(0, info['deleted'])
        self.assertEquals(info['modified'], info['modified_deleted'])

        info2 = client.delete(ENCODED)
        self.assertNotEquals(0, info2['deleted'])
        self.assertTrue(info2['modified_deleted'] > info['modified_deleted'])

        info3 = client.delete(ENCODED, 100)
        self.assertNotEquals(0, info3['deleted'])
        self.assertTrue(info3['modified_deleted'] > info2['modified_deleted'])

        info4 = client.delete(ENCODED, deleted=info3['deleted'])
        self.assertEquals(info3['deleted'], info4['deleted'])
        self.assertTrue(info4['modified_deleted'] > info3['modified_deleted'])

        time.sleep(0.2)
        info5 = client.delete(ENCODED,
            modified_deleted=info4['modified_deleted'] - 1)
        self.assertEquals(info4['deleted'], info5['deleted'])
        self.assertEquals(info4['modified_deleted'], info5['modified_deleted'])

    def test_undelete(self):
        client = clblob.client.Client(self.config)
        client.put(NAME, 'undelete me')
        info = client.delete(ENCODED, 100)
        self.assertNotEquals(0, info['deleted'])
        info = client.delete(ENCODED, None)
        self.assertEquals(0, info['deleted'])
        info = client.delete(ENCODED, 100)
        self.assertNotEquals(0, info['deleted'])
        info = client.delete(ENCODED, deleted=0)
        self.assertEquals(0, info['deleted'])

    def test_delete_before_put(self):
        client = clblob.client.Client(self.config)
        info = client.delete(ENCODED)
        self.assertEquals(None, info['modified'])
        self.assertNotEquals(0, info['deleted'])
        time.sleep(0.2)
        self.assertRaises(clblob.NotFound, client.get, ENCODED)
        info2 = client.put(NAME, 'test data')
        time.sleep(0.2)
        self.assertNotEquals(None, info2['modified'])
        self.assertEquals(0, info2['deleted'])
        self.assertNotEquals(info['modified_deleted'],
            info2['modified_deleted'])

    def test_delete_before_old_put(self):
        client = clblob.client.Client(self.config)
        info = client.delete(ENCODED)
        time.sleep(0.2)
        info2 = client.put(NAME, 'test data',
            modified=info['modified_deleted'] - 1)
        time.sleep(0.2)
        self.assertNotEquals(None, info2['modified'])
        self.assertNotEquals(0, info2['deleted'])
        self.assertEquals(info['modified_deleted'], info2['modified_deleted'])

    def test_get(self):
        client = clblob.client.Client(self.config)
        data = str(range(100000))
        data_hash = hashlib.md5(data).hexdigest()  # pylint: disable=E1101
        info = client.put(NAME, data)

        time.sleep(0.2)
        data2 = client.get(ENCODED).read()
        self.assertEquals(data_hash,
            hashlib.md5(data2).hexdigest())  # pylint: disable=E1101

        self.assertEquals(info, client.get(ENCODED, 'info'))
        self.assertRaises(clblob.RequestError, client.get, ENCODED, 'bad')
        self.assertRaises(clblob.NotFound, client.get, '00102_test5')
        self.assertRaises(clblob.NotFound, client.get, '00102_test5', 'info')

    def test_name(self):
        client = clblob.client.Client(self.config)
        self.assertEquals('00001_1', client.name('1'))
        self.assertEquals('00102_\xc3\xbc', client.name(u'ü'))
        self.assertEquals(u'00102_ü', client.name(u'ü').decode('utf-8'))
        self.assertEquals('00002_\xc3\xb1', client.name('\xc3\xb1'))
        self.assertEquals(u'00002_\xf1',
            client.name('\xc3\xb1').decode('utf-8'))
        self.assertRaises(clblob.InvalidRequest, client.get, '10100_test1')
        self.assertRaises(clblob.InvalidRequest, client.get, '001000_test1')
        self.assertRaises(clblob.InvalidRequest, client.get, {})
        self.assertRaises(clblob.InvalidRequest, client.get, '')
        self.assertRaises(clblob.InvalidRequest, client.get, '_test')
        self.assertRaises(clblob.InvalidRequest, client.get, '\xfc')

    def test_put(self):
        client = clblob.client.Client(self.config)
        data = str(range(100000))
        data_hash = hashlib.md5(data).hexdigest()  # pylint: disable=E1101

        self.assertRaises(clblob.RequestError, client.put, NAME, [])

        for replica in REPLICAS + OTHERS:
            self.assertEquals(False, os.path.isfile(PATH % replica))
        info = client.put(NAME, data)
        self.assertEquals(ENCODED, info['name'])
        self.assertNotEquals(0, info['modified'])
        time.sleep(0.2)
        for replica in REPLICAS:
            self.assertEquals(True, os.path.isfile(PATH % replica))
            data2 = open(PATH % replica).read()
            self.assertEquals(len(data), len(data2))
            self.assertEquals(data_hash,
                hashlib.md5(data2).hexdigest())  # pylint: disable=E1101
        for replica in OTHERS:
            self.assertEquals(False, os.path.isfile(PATH % replica))

        info2 = client.put(ENCODED, data, encoded=True)
        self.assertEquals(info['name'], info2['name'])
        self.assertNotEquals(info['modified'], info2['modified'])
        time.sleep(0.2)
        for replica in REPLICAS:
            self.assertEquals(True, os.path.isfile(PATH % replica))
            data2 = open(PATH % replica).read()
            self.assertEquals(len(data), len(data2))
            self.assertEquals(data_hash,
                hashlib.md5(data2).hexdigest())  # pylint: disable=E1101
        for replica in OTHERS:
            self.assertEquals(False, os.path.isfile(PATH % replica))

        info = client.put(NAME, 'test data', modified=info2['modified'] + 1)
        info2 = client.put(NAME, 'test data', modified=info['modified'])
        self.assertEquals(info, info2)

    def test_put_chunk(self):
        client = clblob.client.Client(self.config)
        data = str(range(100000))
        data_hash = hashlib.md5(data).hexdigest()  # pylint: disable=E1101
        client.put(NAME, StringIO.StringIO(data))
        time.sleep(0.2)
        for replica in REPLICAS:
            self.assertEquals(True, os.path.isfile(PATH % replica))
            data2 = open(PATH % replica).read()
            self.assertEquals(len(data), len(data2))
            self.assertEquals(data_hash,
                hashlib.md5(data2).hexdigest())  # pylint: disable=E1101

    def test_put_file(self):
        client = clblob.client.Client(self.config)
        data = open('README.rst').read()
        data_hash = hashlib.md5(data).hexdigest()  # pylint: disable=E1101
        client.put(NAME, open('README.rst'))
        time.sleep(0.2)
        for replica in REPLICAS:
            self.assertEquals(True, os.path.isfile(PATH % replica))
            data2 = open(PATH % replica).read()
            self.assertEquals(len(data), len(data2))
            self.assertEquals(data_hash,
                hashlib.md5(data2).hexdigest())  # pylint: disable=E1101

    def test_put_old(self):
        client = clblob.client.Client(self.config)
        info = client.put(NAME, 'test data')
        time.sleep(0.2)
        info2 = client.put(NAME, 'old test data',
            modified=info['modified'] - 1)
        self.assertEquals(info['modified'], info2['modified'])
        time.sleep(0.2)
        self.assertEquals('test data', client.get(ENCODED).read())

    def test_put_bad_path(self):
        client = clblob.client.Client(self.config)
        open('test_blob/010/6b', 'w').write('should be a dir')
        client.put(NAME, 'test data')

    def test_put_bad_data(self):
        replica = self.config['clblob']['client']['replicas']['010']
        connection = httplib.HTTPConnection(replica['ip'], replica['port'])
        connection.request('PUT', '/%s' % NAME, 'test', {'Content-Length': 10})
        time.sleep(0.2)
        self.assertEquals(1, len(os.listdir('test_blob/010/_tmp')))
        connection.sock.shutdown(socket.SHUT_WR)
        response = connection.getresponse()
        self.assertEquals(500, response.status)
        self.assertEquals(0, len(os.listdir('test_blob/010/_tmp')))

    def test_replicas(self):
        client = clblob.client.Client(self.config)
        self.assertEquals(REPLICAS0, client.replicas(NAME))
        self.assertEquals(REPLICAS0, client.replicas(ENCODED, True))

    def test_buffer(self):
        replicas = ['000', '010']
        others = [server for server in self.servers if server not in replicas]
        for server in others:
            self.servers[server].stop()
        client = clblob.client.Client(self.config)
        names = []
        for count in xrange(20):
            names.append(client.put(str(count), str(count))['name'])
        time.sleep(0.2)
        for server in replicas:
            self.servers[server].stop()
        for server in others:
            self.servers[server].start()
        for name in names:
            self.assertRaises(clblob.RequestError, client.get, name)
        for server in replicas:
            self.servers[server].start()
        for _count in xrange(3):
            for server in replicas:
                client.buffer(server)
            time.sleep(0.2)
        for server in replicas:
            self.servers[server].stop()
        for count, name in enumerate(names):
            self.assertEquals(str(count), client.get(name).read())
        for server in replicas:
            self.servers[server].start()

    def test_configcheck(self):
        config = clcommon.config.update(self.config, {
            'clblob': {
                'store': {
                    'disk': {
                        'path': 'test_blob'}},
                'index': {
                    'sqlite': {
                        'database': 'test_blob/_index'}},
                'client': {
                    'bucket_count_mismatch_catastrophic': True,
                    'churn_threshold': 0.5,
                    'cluster': '0',
                    'clusters': [
                        [{'replicas': ['000', '001', '002'],
                            'write_weight': 1}]],
                    'replica': '000'}}})

        client = clblob.client.Client(config)
        configcheck = client.configcheck(config, '000')
        self.assertEquals(0, len(configcheck['catastrophic']))
        self.assertEquals(0, len(configcheck['reconfig']))
        self.assertEquals(0, len(configcheck['churn']))
        self.assertEquals(0, len(configcheck['clusters']['added']))
        self.assertEquals(0, len(configcheck['clusters']['removed']))

        replace_replica = clcommon.config.update_option(config,
            'clblob.client.clusters',
            [[{'replicas': ['003', '001', '002'], 'write_weight': 1}]])
        configcheck = client.configcheck(replace_replica, '000')
        self.assertEquals(0, len(configcheck['catastrophic']))
        self.assertEquals(2, len(configcheck['reconfig']))
        self.assertEquals(1,
            len(configcheck['clusters']['added']['replicas'][0][0]))
        self.assertEquals((1.0 / 3.0), configcheck['churn'][0][0])
        self.assertEquals(1,
            len(configcheck['clusters']['removed']['replicas'][0][0]))

        encode_name_reconfig = clcommon.config.update_option(replace_replica,
            'clblob.client.encode_name', False)
        configcheck = client.configcheck(encode_name_reconfig, '000')
        self.assertEquals(2, len(configcheck['catastrophic']))

        remove_cluster = clcommon.config.update_option(config,
            'clblob.client.clusters', [[]])
        configcheck = client.configcheck(remove_cluster, '000')
        self.assertEquals(1, len(configcheck['catastrophic']))
        self.assertEquals(1,
            len(configcheck['clusters']['removed']['buckets']))

        insert_bucket = clcommon.config.update_option(config,
            'clblob.client.clusters', [[
                {'replicas': ['100', '101', '102'], 'write_weight': 1},
                {'replicas': ['000', '001', '002'], 'write_weight': 1}
            ]])
        configcheck = client.configcheck(insert_bucket, '000')
        self.assertEquals(1, len(configcheck['catastrophic']))
        self.assertEquals(1,
            len(configcheck['clusters']['added']['buckets'][0]))
        self.assertEquals(1.0, configcheck['churn'][0][0])

        bucket_count_mismatch = clcommon.config.update_option(config,
            'clblob.client.clusters', [[
                {'replicas': ['000', '001', '002'], 'write_weight': 1}
            ], [
                {'replicas': ['100', '101', '102'], 'write_weight': 1},
                {'replicas': ['200', '201', '202'], 'write_weight': 1}
            ]])
        configcheck = client.configcheck(bucket_count_mismatch, '000')
        self.assertEquals(1, len(configcheck['catastrophic']))
        self.assertEquals(True, configcheck['bucket_count_mismatch'])

        client.stop()

    def test_list(self):
        client = clblob.client.Client(self.config)
        self.assertEquals([], client.list('010'))
        self.assertEquals([], client.list('010', checksum=False))

        info = client.put(NAME, 'test data')
        time.sleep(0.2)
        result = client.list('010', checksum=False)
        self.assertEquals(1, len(result))
        self.assertEquals(info['name'], result[0][0])
        self.assertEquals(info['modified'], result[0][1])
        self.assertEquals(info['deleted'], result[0][2])
        self.assertEquals(info['modified_deleted'], result[0][3])
        self.assertEquals(0, len(client.list('010', modified_start=0,
            modified_stop=1, checksum=False)))
        self.assertEquals(1, len(client.list('010', modified_start=0,
            modified_stop=2 ** 63 - 1, checksum=False)))

        for count in xrange(20):
            modified = info['modified'] + count * (50000 << 32)
            client.put(str(count), str(count), modified=modified)
        time.sleep(0.2)
        self.assertNotEquals([], client.list('010'))
        self.assertNotEquals([], client.list('010', checksum=False))

    def test_purge(self):
        client = clblob.client.Client(self.config)
        client.put(NAME, 'purge me')
        time.sleep(0.2)
        for replica in REPLICAS:
            self.assertEquals(True, os.path.isfile(PATH % replica))
        client.delete(ENCODED, -200)
        time.sleep(0.2)
        for replica in REPLICAS:
            client.purge(replica)
        time.sleep(0.2)
        for replica in REPLICAS:
            self.assertEquals(False, os.path.isfile(PATH % replica))

    def test_purge_worker(self):
        client = clblob.client.Client(self.config)
        client.put(NAME, 'test data')
        time.sleep(0.2)
        for replica in REPLICAS:
            self.assertEquals(True, os.path.isfile(PATH % replica))
        client.delete(ENCODED, -200)
        time.sleep(2)
        for replica in REPLICAS:
            self.assertEquals(False, os.path.isfile(PATH % replica))

    def test_purge_bad_blob(self):
        client = clblob.client.Client(self.config)
        client.put(NAME, 'test data')
        time.sleep(0.2)
        for replica in REPLICAS:
            self.assertEquals(True, os.path.isfile(PATH % replica))
        os.unlink(PATH % '010')
        os.mkdir(PATH % '010')
        client.delete(ENCODED, -200)
        time.sleep(0.2)
        client.purge('010')
        time.sleep(0.2)
        self.assertEquals(True, os.path.isdir(PATH % '010'))
        os.rmdir(PATH % '010')
        client.purge('010')

    def test_status(self):
        client = clblob.client.Client(self.config)
        status = client.status('010')
        self.assertEquals('010', status['replica'])
        self.servers['011'].stop()
        self.servers['012'].stop()
        client.put(NAME, 'test data')
        time.sleep(0.2)
        status = client.status('010')
        self.assertTrue('last_failed' in status['replicas']['011'])
        self.servers['011'].start()
        self.servers['012'].start()

    def test_sync(self):
        client = clblob.client.Client(self.config)
        names = []
        for count in xrange(20):
            names.append(client.put(str(count), 'test')['name'])
        time.sleep(0.2)
        self.servers['010'].stop()
        shutil.rmtree(os.path.join('test_blob', '010'))
        os.makedirs(os.path.join('test_blob', '010'))
        self.servers['010'].start()
        client.sync('010')
        time.sleep(0.2)
        self.servers['011'].stop()
        self.servers['012'].stop()
        for name in names:
            self.assertEquals('test', client.get(name).read())
        self.servers['011'].start()
        self.servers['012'].start()

    def test_sync_one(self):
        client = clblob.client.Client(self.config)
        for count in xrange(20):
            client.put(str(count), 'test')
        time.sleep(0.2)
        names = [info[0] for info in client.list('010', checksum=False)]
        self.servers['010'].stop()
        index = sqlite3.connect('test_blob/010/_index')
        index.execute('DELETE FROM blob WHERE name=?', (names[0],))
        index.commit()
        index.close()
        self.servers['010'].start()
        client.sync('010')
        time.sleep(0.2)
        found = False
        for name in client.list('010', checksum=False):
            if name[0] == names[0]:
                found = True
                break
        self.assertEquals(True, found)

    def test_replica_retry(self):
        client = clblob.client.Client(self.config)
        self.servers['010'].stop()
        client.put(NAME, 'test data')
        time.sleep(0.2)
        self.assertEquals(False, os.path.isfile(PATH % '010'))
        self.servers['010'].start()
        time.sleep(1.2)
        self.assertEquals(True, os.path.isfile(PATH % '010'))

    def test_cluster_retry(self):
        client = clblob.client.Client(self.config)
        for replica in REPLICAS1:
            self.servers[replica].stop()
        client.put(NAME, 'test data')
        time.sleep(0.2)
        for replica in REPLICAS1:
            self.assertEquals(False, os.path.isfile(PATH % replica))
        for replica in REPLICAS1:
            self.servers[replica].start()
        time.sleep(1.2)
        for replica in REPLICAS1:
            self.assertEquals(True, os.path.isfile(PATH % replica))

    def test_replica_retry_restart(self):
        client = clblob.client.Client(self.config)
        self.servers['010'].stop()
        client.put(NAME, 'test data')
        time.sleep(0.2)
        self.assertEquals(False, os.path.isfile(PATH % '010'))
        self.servers['011'].stop()
        self.servers['012'].stop()
        for replica in REPLICAS0:
            self.servers[replica].start()
        time.sleep(1.2)
        self.assertEquals(True, os.path.isfile(PATH % '010'))

    def test_cluster_retry_restart(self):
        client = clblob.client.Client(self.config)
        for replica in REPLICAS1:
            self.servers[replica].stop()
        client.put(NAME, 'test data')
        time.sleep(0.2)
        for replica in REPLICAS1:
            self.assertEquals(False, os.path.isfile(PATH % replica))
        for replica in REPLICAS0:
            self.servers[replica].stop()
            self.servers[replica].start()
        for replica in REPLICAS1:
            self.servers[replica].start()
        time.sleep(1.2)
        for replica in REPLICAS1:
            self.assertEquals(True, os.path.isfile(PATH % replica))

    def test_commit_uncommitted(self):
        index = self.servers['010'].blob_client._index  # pylint: disable=W0212
        update = index.update

        def _update(*args, **kwargs):
            '''Wrapper to call index.update and then raise an exception.'''
            update(*args, **kwargs)
            raise RuntimeError('Force raise from test')
        index.update = _update
        client = clblob.client.Client(self.config)
        client.put(NAME, 'test data')
        time.sleep(0.2)
        self.assertEquals(False, os.path.isfile(PATH % '010'))
        self.servers['010'].stop()
        self.servers['010'].start()
        self.assertEquals(True, os.path.isfile(PATH % '010'))

    def test_abort_uncommitted(self):
        index = self.servers['010'].blob_client._index  # pylint: disable=W0212

        def _update(*_args, **_kwargs):
            '''Wrapper to call raise an exception instead of calling update.'''
            raise RuntimeError('Force raise from test')
        index.update = _update
        client = clblob.client.Client(self.config)
        client.put(NAME, 'test data')
        time.sleep(0.2)
        self.assertEquals(False, os.path.isfile(PATH % '010'))
        self.servers['010'].stop()
        self.servers['010'].start()
        self.assertEquals(False, os.path.isfile(PATH % '010'))

    def test_unicode(self):
        client = clblob.client.Client(self.config)
        info = client.put(u'☮', u'peace!')
        self.assertEquals(u'00101_☮', info['name'])
        self.assertEquals('peace!', client.get(info['name']).read())
        self.assertEquals(info, client.get(info['name'], 'info'))
        client.delete(info['name'])
        self.assertRaises(clblob.RequestError, client.put, '1', u'☮')


class TestClientNoThreads(TestClient):

    config = clcommon.config.update(test.CONFIG, {
        'clblob': {
            'index': {'sqlite': {'thread': False}},
            'store': {'disk': {'pool_size': 0}}}})


class TestClientSync(test.Base):

    config = clcommon.config.update(test.CONFIG, {
        'clblob': {
            'index': {'sqlite': {'sync': 2}},
            'store': {'disk': {'sync': True}}}})

    def test_sync(self):
        client = clblob.client.Client(self.config)
        info = client.put(NAME, 'test data')
        self.assertEquals(ENCODED, info['name'])
        time.sleep(0.5)
        self.assertEquals('test data', client.get(ENCODED).read())
