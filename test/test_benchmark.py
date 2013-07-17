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

'''Tests for craigslist blob benchmark module.'''

import clblob.benchmark
import clcommon.config
import test

CONFIG = clcommon.config.update(clblob.benchmark.DEFAULT_CONFIG, {
    'clblob': {
        'benchmark': {
            'deletes_per_thread': 10,
            'gets_per_thread': 10,
            'puts_per_thread': 10}}},
    test.CONFIG)


class TestBenchmark(test.Base):

    config = CONFIG

    def test_run(self):
        benchmark = clblob.benchmark.Benchmark(self.config)
        benchmark.run()
        for method in ['delete', 'get', 'put']:
            self.assertEquals(1, benchmark.results[method]['threads'])
            self.assertEquals(10, benchmark.results[method]['calls'])

    def test_run_no_delete(self):
        config = clcommon.config.update_option(self.config,
            'clblob.benchmark.delete_threads', 0)
        benchmark = clblob.benchmark.Benchmark(config)
        benchmark.run()
        self.assertTrue('delete' not in benchmark.results)
        for method in ['get', 'put']:
            self.assertEquals(1, benchmark.results[method]['threads'])
            self.assertEquals(10, benchmark.results[method]['calls'])

    def test_run_no_put(self):
        config = clcommon.config.update_option(self.config,
            'clblob.benchmark.put_threads', 0)
        self.assertRaises(clcommon.config.ConfigError,
            clblob.benchmark.Benchmark, config)

    def test_run_no_servers(self):
        for name in list(self.servers):
            self.servers[name].stop()
            del self.servers[name]
        benchmark = clblob.benchmark.Benchmark(self.config)
        self.assertEquals(0, len(benchmark.results))


class TestBenchmarkPatched(TestBenchmark):

    config = clcommon.config.update_option(CONFIG,
        'clblob.benchmark.patched_threads', True)
