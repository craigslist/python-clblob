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

'''craigslist blob store disk module.

This module allows for persistent storage of blobs as files on an existing,
user-writable filesystem. You can control the durability of blobs by
using the sync option, which enables calling fdatasync after the blob has
been written (this can slow things down significantly for write-heavy
workloads, but data will most likely persistent across power failures
and other errors where kernel buffers were not flushed to the filesystem).

All puts are written to a temporary directory while being received and
moved into the final blob storage location on commit. Any uncommitted
blobs are cleaned up the next time the store is initialized.'''

import errno
import hashlib
import os.path
import time

import clblob
import clcommon.config
import clcommon.log
import clcommon.worker

DEFAULT_CONFIG = {
    'clblob': {
        'store': {
            'disk': {
                'buffer_size': 131072,
                'log_level': 'NOTSET',
                'path': None,
                'path_depth': 2,
                'pool_size': 4,
                'sync': True}}}}


class Store(object):
    '''Disk store class.'''

    def __init__(self, client, event):
        self.config = client.root_config['clblob']['store']['disk']
        self.log = clcommon.log.get_log('clblob_store_disk',
            self.config['log_level'])
        if self.config['path'] is None:
            raise clcommon.config.ConfigError(_('Path must be set'))
        if not os.path.isdir(self.config['path']):
            raise clcommon.config.ConfigError(_('Path must be directory: %s') %
                self.config['path'])
        if not os.access(self.config['path'], os.W_OK):
            raise clcommon.config.ConfigError(_('Path must be writable: %s') %
                self.config['path'])
        replica_file = os.path.join(self.config['path'], '_replica')
        if os.path.exists(replica_file):
            path_replica = open(replica_file).read()
            if client.replica != path_replica:
                raise clcommon.config.ConfigError(
                    _('Path not used for given replica: %s != %s '
                      '(remove %s if you really want to do this)') %
                    (client.replica, path_replica, replica_file))
        else:
            open(replica_file, 'w').write(client.replica)
        tmp_dir = os.path.join(self.config['path'], '_tmp')
        try:
            os.mkdir(tmp_dir)
        except OSError, exception:
            if exception.errno != errno.EEXIST:
                raise
        self.log.info(_('Starting the store pool'))
        self._pool = clcommon.worker.Pool(self.config['pool_size'])
        event.profile.mark_time('store_start:%s' % event.method)

    def _run(self, method, event, *args, **kwargs):
        '''Queue the store method to be run on the pool and wait for
        the result. Also mark the time it takes to get the result back.'''
        result = self._pool.start(self._run_pool, method, event, args,
            kwargs).wait()
        event.profile.mark_time('store_return:%s' % event.method)
        return result

    @staticmethod
    def _run_pool(method, event, args, kwargs):
        '''Run the store method in the pool and mark profile times.'''
        event.profile.mark_time('store_queue:%s' % event.method)
        result = method(event, *args, **kwargs)
        event.profile.mark_time('store_%s:%s' %
            (method.__name__[1:], event.method))
        return result

    def stop(self):
        '''Stop the disk pool.'''
        self.log.info(_('Stopping the store pool'))
        self._pool.stop()

    def get_uncommitted(self, event):
        '''Get modified values for any uncommitted blobs.'''
        return self._run(self._get_uncommitted, event)

    def _get_uncommitted(self, _event):
        '''Version of method that runs in the store pool.'''
        path = os.path.join(self.config['path'], '_tmp')
        return [(int(modified.split('.', 1)[0]), os.path.join(path, modified))
            for modified in os.listdir(path)]

    def delete(self, event):
        '''Delete blob from disk.'''
        event.store_id = self._name_path(event.name)
        self._run(self._delete, event)

    @staticmethod
    def _delete(event):
        '''Version of method that runs in the store pool.'''
        try:
            os.unlink(event.store_id)
        except OSError, exception:
            if exception.errno != errno.ENOENT:
                raise
        event.store_id = None

    def get(self, event):
        '''Get an object for streaming a blob.'''
        event.store_id = self._name_path(event.name)
        blob_file = self._run(self._open, event)
        return _Stream(self, event, blob_file)

    def _open(self, event, mode='rb'):
        '''Open a file for writing.'''
        try:
            return open(event.store_id, mode, 0)
        except IOError, exception:
            if exception.errno == errno.ENOENT:
                message = _('Could not open: %s') % event.store_id
                self.log.warning(message)
                raise clblob.NotFound(message)
            raise

    def put(self, event):
        '''Try saving the file to the tmp directory, and if anything goes
        wrong, remove it.'''
        try:
            self._put(event)
        except Exception:
            self._put_abort(event)
            raise

    def _put(self, event):
        '''Put the blob associated with an event into the tmp directory. If
        the amount of data read or in the buffer is within the configured
        buffer size, keep this cached data around for other events to
        use. If it is not, set to 'True' which means to stream the file
        from disk if it needs to be read again.'''
        event.store_id = os.path.join(self.config['path'], '_tmp',
            '%d.%f' % (event.modified, time.time()))
        blob_file = self._run(self._open, event, 'wb')
        cached_data = ''
        if hasattr(event.data, 'read'):
            total_len = 0
            while True:
                chunk = event.data.read(self.config['buffer_size'])
                event.profile.mark_time('store_read_source:%s' % event.method)
                if not chunk:
                    break
                self._run(self._write, event, blob_file, chunk)
                total_len += len(chunk)
                if total_len <= self.config['buffer_size']:
                    cached_data += chunk
                else:
                    cached_data = True
        else:
            self._run(self._write, event, blob_file, event.data)
            if len(event.data) <= self.config['buffer_size']:
                cached_data = event.data
            else:
                cached_data = True
        event.data = cached_data
        self._run(self._close, event, blob_file, True)

    def _put_abort(self, event):
        '''Abort the put, but ignore any errors that may be encountered.'''
        try:
            self.abort(event)
        except Exception:
            pass

    @staticmethod
    def _write(event, blob_file, chunk):
        '''Write a chunk of data to the file.'''
        blob_file.write(chunk)
        event.profile.mark('store_write:%s:size' % event.method, len(chunk))

    def _close(self, event, blob_file, sync=False):
        '''Sync the file if needed and close the file.'''
        if sync and self.config['sync']:
            os.fdatasync(blob_file)
            event.profile.mark_time('store_fdatasync:%s' % event.method)
        blob_file.close()

    def commit(self, event):
        '''Commit the blob for an event by moving it to its final location.'''
        self._run(self._commit, event)

    def _commit(self, event):
        '''Version of method that runs in the store pool.'''
        path = self.config['path']
        for part in self._name_path_parts(event.name):
            path = os.path.join(path, part)
            try:
                os.mkdir(path)
            except OSError, exception:
                if exception.errno != errno.EEXIST:
                    raise
        destination = os.path.join(path, event.name)
        os.rename(event.store_id, destination)
        event.store_id = destination

    def abort(self, event):
        '''Abort a pending put command for an event.'''
        self._run(self._delete, event)

    @staticmethod
    def logstatus(event, status):
        '''Mark metrics for the store.'''
        status = status['store_disk']
        event.profile.mark('store_disk_queue_size', status['queue_size'])
        usage = 1 - float(status['free_space']) / status['total_space']
        event.profile.mark('store_disk_usage', int(100 * usage))
        usage = 1 - float(status['free_inodes']) / status['total_inodes']
        event.profile.mark('store_disk_inode_usage', int(100 * usage))

    def status(self, event, status):
        '''Get status info for the store.'''
        status['store_disk'] = {}
        status = status['store_disk']
        status['queue_size'] = self._pool.qsize()
        status['path'] = self.config['path']
        self._run(self._status, event, status)

    def _status(self, _event, status):
        '''Version of method that runs in the store pool.'''
        vfsstat = os.statvfs(self.config['path'])
        status['total_space'] = vfsstat.f_bsize * vfsstat.f_blocks
        status['free_space'] = vfsstat.f_bsize * vfsstat.f_bavail
        status['total_inodes'] = vfsstat.f_files
        status['free_inodes'] = vfsstat.f_favail

    def _name_path_parts(self, name):
        '''Get the path parts for a name.'''
        name_hash = hashlib.md5(name).hexdigest()  # pylint: disable=E1101
        return [name_hash[offset * 2:offset * 2 + 2]
            for offset in xrange(self.config['path_depth'])]

    def _name_path(self, name):
        '''Get the path for a name.'''
        parts = self._name_path_parts(name)
        parts.insert(0, self.config['path'])
        parts.append(name)
        return os.path.join(*parts)


class _Stream(object):
    '''File-like object for streaming that run methods in the store pool.'''
    # pylint: disable=W0212

    def __init__(self, store, event, blob_file):
        self._store = store
        self._event = event
        self._blob_file = blob_file

    @property
    def content_length(self):
        '''Get the size of the blob being streamed.'''
        return self._store._run(self._stat, self._event).st_size

    def _stat(self, _event):
        '''Get the stat structure for the file.'''
        return os.fstat(self._blob_file.fileno())

    def read(self, size=-1):
        '''Read a chunk of data from the file.'''
        if 0 < size < self._store.config['buffer_size']:
            size = self._store.config['buffer_size']
        return self._store._run(self._read, self._event, size)

    def _read(self, event, size):
        '''Version of method that runs in the store pool.'''
        data = self._blob_file.read(size)
        event.profile.mark('store_read:%s:size' % event.method, len(data))
        return data

    def close(self):
        '''Close the file.'''
        self._store._run(self._store._close, self._event, self._blob_file)

    def __iter__(self):
        return self

    def next(self):
        '''Return the next chunk of the file for iterator.'''
        data = self._store._run(self._read, self._event,
            self._store.config['buffer_size'])
        if not data:
            raise StopIteration
        return data
