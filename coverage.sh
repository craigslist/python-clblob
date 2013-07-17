#!/bin/sh
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

echo "+++ Preparing to run coverage"
coverage=`which coverage`
if [ -z $coverage ]; then
    coverage=`which python-coverage`
    if [ -z $coverage ]; then
        echo 'Python coverage not found'
        exit 1
    fi
fi

cd `dirname "$0"`
export PYTHONPATH=".:$PYTHONPATH"
rm -rf coverage.html .coverage*
echo

echo "+++ Running test suite"
$coverage run -p setup.py nosetests
echo

echo "+++ Running commands"
rm -rf test_blob test_blob_data
mkdir -p test_blob
echo "test blob data" > test_blob_data
blob_config='
    --clblob.client.clusters=[[{"replicas":["test"],"write_weight":1}]]
    --clblob.client.encode_name=false
    --clblob.client.replica=test
    --clblob.client.replicas={"test":{"ip":"127.0.0.1","port":12345,"read_weight":1}}
    --clblob.index.sqlite.database=test_blob/_index
    --clblob.index.sqlite.sync=0
    --clblob.store.disk.path=test_blob
    --clblob.store.disk.sync=false'
$coverage run -p clblob/benchmark.py -n $blob_config
$coverage run -p clblob/benchmark.py -n $blob_config \
    --clblob.benchmark.patched_threads=true
$coverage run -p clblob/client.py -n put README.rst
$coverage run -p clblob/client.py -n $blob_config
$coverage run -p clblob/client.py -n $blob_config \
    --clblob.client.replica=null status
$coverage run -p clblob/client.py -n $blob_config put test_blob_data
$coverage run -p clblob/client.py -n $blob_config put test_blob_data 0
$coverage run -p clblob/client.py -n $blob_config put test_blob_data deleted=0
$coverage run -p clblob/client.py -n $blob_config get test_blob_data
echo

for signal in 2 9 15; do
    echo "+++ Testing clblobserver shutdown with kill -$signal"
    $coverage run -p clblob/server.py -n $blob_config \
        --clcommon.http.port=12342 \
        --clcommon.log.syslog_ident=test \
        --clcommon.server.daemonize=true \
        --clcommon.server.pid_file=test_pid
    sleep 0.2
    kill -$signal `cat test_pid`
    sleep 1.2
    rm test_pid
done
echo

echo "+++ Generating coverage report"
$coverage combine
$coverage html -d coverage.html --include='clblob/*'
$coverage report --include='clblob/*'
