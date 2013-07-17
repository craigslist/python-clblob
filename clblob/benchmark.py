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

'''craigslist blob benchmark module.

The blob service contains a python module for benchmarking the performance
of the various kinds of blob requests. This can be used from the command
line tool clblobbencmark or through importing this module and using the
Benchmark class. Benchmark runs must first put at least one blob before
the delete and get operations can start since they work off of the list
of blob names that put has completed. Blob names are the checksum of
the blob content so get requests can verify the content.

The minimum and maximum blob size is also configurable to best match a
predicted workload. The amount of concurrency and number of requests per
thread for each operation is also controlled through config options. Any
operation can be disabled by setting the thread count to 0. This tool
can use both real or patched threads (currently gevent greenlets).'''

import hashlib
import random
import StringIO
import time

import clblob.client
import clcommon.config
import clcommon.log
import clcommon.profile
import clcommon.worker

DEFAULT_CONFIG = clcommon.config.update(clblob.client.DEFAULT_CONFIG, {
    'clblob': {
        'benchmark': {
            'delete_threads': 1,
            'deletes_per_thread': 100,
            'get_threads': 1,
            'gets_per_thread': 100,
            'log_level': 'NOTSET',
            'min_size': 0,
            'max_size': 262144,
            'patched_threads': False,
            'put_threads': 1,
            'puts_per_thread': 100}}})

DEFAULT_CONFIG_FILES = clblob.client.DEFAULT_CONFIG_FILES + [
    '/etc/clblobbenchmark.conf',
    '~/.clblobbenchmark.conf']

DEFAULT_CONFIG_DIRS = clblob.client.DEFAULT_CONFIG_DIRS + [
    '/etc/clblobbenchmark.d',
    '~/.clblobbenchmark.d']


class Benchmark(object):
    '''This class manages running a benchmark against a blob
    service. Results from runs are stored in the *results* attribute,
    along with full profile information in the *profile_log* attribute
    (to be used with clcommon.profile).'''

    def __init__(self, config):
        self.config = config['clblob']['benchmark']
        if self.config['put_threads'] == 0:
            raise clcommon.config.ConfigError(
                _('Must have at least one put thread'))
        if self.config['patched_threads']:
            import gevent.monkey
            gevent.monkey.patch_all()
        self.log = clcommon.log.get_log('clblob_benchmark',
            self.config['log_level'])
        self._client = clblob.client.Client(config)
        self._size_range = self.config['max_size'] - self.config['min_size']
        self._data = ''.join(chr(byte % 256)
            for byte in xrange(self.config['max_size']))
        self._running = False
        self.names = []
        self.profile_logs = []
        self.profile_log = StringIO.StringIO()
        self.results = {}

    def run(self):
        '''Run the benchmark.'''
        self.names = []
        self.profile_logs = []
        self.profile_log.truncate()
        self.results = {}
        self._run_threads()
        for log in self.profile_logs:
            self.profile_log.write(log)
        self.profile_log.seek(0)
        report_data = clcommon.profile.report_data(self.profile_log)
        for method in ['delete', 'get', 'put']:
            method_time = report_data.get('%s:time' % method)
            if method_time is None:
                continue
            thread_rate = 1.0 / method_time['average']
            self.results[method] = {
                'calls': method_time['count'],
                'threads': self.config['%s_threads' % method],
                'thread_rate': thread_rate,
                'total_rate': thread_rate * self.config['%s_threads' % method]}
        self.profile_log.seek(0)

    def _run_threads(self):
        '''Start threads for a run and wait until they complete.'''
        pool = clcommon.worker.Pool(self.config['delete_threads'] +
            self.config['get_threads'] + self.config['put_threads'], True)
        batch = pool.batch()
        self._running = True
        for _count in xrange(self.config['delete_threads']):
            batch.start(self._delete_thread)
        for _count in xrange(self.config['get_threads']):
            batch.start(self._get_thread)
        for _count in xrange(self.config['put_threads']):
            batch.start(self._put_thread)
        batch.wait_all()

    def _delete_thread(self):
        '''Run the configured number of delete request. This blocks until
        the put thread has inserted at least one blob so it has a blob
        name to delete.'''
        while len(self.names) == 0:
            if not self._running:
                return
            time.sleep(0.1)
        for count in xrange(self.config['deletes_per_thread']):
            name = self.names[count % len(self.names)]
            profile = clcommon.profile.Profile()
            try:
                self._client.delete(name)
                profile.mark_time('delete')
            except Exception, exception:
                self.log.warning(_('Delete request failed: %s'), exception)
                profile.mark('delete:fail', 1)
            self.profile_logs.append('%s\n' % profile)

    def _get_thread(self):
        '''Run the configured number of get request. This blocks until the
        put thread has inserted at least one blob so it has a blob name
        to get. The blob content retrieved is verified with a checksum
        by using the blob name (which contains the checksum).'''
        while len(self.names) == 0:
            if not self._running:
                return
            time.sleep(0.1)
        for count in xrange(self.config['gets_per_thread']):
            name = self.names[count % len(self.names)]
            profile = clcommon.profile.Profile()
            data = None
            try:
                data = self._client.get(name).read()
                profile.mark_time('get')
                profile.mark('get:size', len(data))
            except Exception, exception:
                self.log.warning(_('Get request failed: %s'), exception)
                profile.mark('get:fail', 1)
            self.profile_logs.append('%s\n' % profile)
            if data is None:
                continue
            checksum = hashlib.md5(data).hexdigest()  # pylint: disable=E1101
            if checksum != name.split('_')[-1:][0]:
                self.log.error(_('Checksum mismatch: %s'), name)

    def _put_thread(self):
        '''Run the configured number of put request. The checksum of the
        blob content is used as the blob name so get requests can verify
        the content.'''
        for _count in xrange(self.config['puts_per_thread']):
            num = random.randrange(2 ** 32)
            size = self.config['min_size'] + (num % self._size_range)
            offset = num % max(1, self.config['max_size'] - size)
            data = self._data[offset:offset + size]
            name = hashlib.md5(data).hexdigest()  # pylint: disable=E1101
            profile = clcommon.profile.Profile()
            try:
                info = self._client.put(name, data)
                profile.mark_time('put')
                profile.mark('put:size', size)
                self.names.append(info['name'])
            except Exception, exception:
                self.log.warning(_('Put request failed: %s'), exception)
                profile.mark('put:fail', 1)
            self.profile_logs.append('%s\n' % profile)
        self._running = False


def _main():
    '''Run the blob benchmark tool.'''
    config = clcommon.config.update(DEFAULT_CONFIG,
        clcommon.log.DEFAULT_CONFIG)
    config, _args = clcommon.config.load(config, DEFAULT_CONFIG_FILES,
        DEFAULT_CONFIG_DIRS, False)
    clcommon.log.setup(config)
    benchmark = Benchmark(config)
    benchmark.run()
    print
    clcommon.profile.report(benchmark.profile_log)
    print
    print _('%35s %8s') % ('Thread', 'Total')
    print _('%8s %8s %8s %8s %8s') % ('Method', 'Count', 'Threads', 'Rate/s',
        'Rate/s')
    for method, result in sorted(benchmark.results.iteritems()):
        print "%8s %8d %8d %8.1f %8.1f" % (method, result['calls'],
            result['threads'], result['thread_rate'], result['total_rate'])


if __name__ == '__main__':
    _main()
