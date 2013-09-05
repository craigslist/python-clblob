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

'''craigslist blob index sqlite module.

This module uses a sqlite database for storing blob info and replication
event data. By default, all index operations run in a separate
thread so IO does not block the main thread for all events. The sync
option allows it to run in different durability modes supported by
SQLite. See http://www.sqlite.org/pragma.html#pragma_synchronous for
more details. SQLite uses a temp directory for some operations, so if
using an disk backed SQLIte database, this should be configured to the
same filesystem the index file lives on. This will prevent errors where
the filesystem that the tmpdir is on fills up and causes the database to
not be able to complete requests.'''

import errno
import os
import sqlite3

import clcommon.config
import clcommon.log
import clcommon.number
import clcommon.worker

DEFAULT_CONFIG = {
    'clblob': {
        'index': {
            'sqlite': {
                'database': None,
                'log_level': 'NOTSET',
                'sync': 2,
                'thread': True,
                'tmpdir': None}}}}

REPLICA_EVENT_FLAGS = dict((name, 1 << count) for count, name in enumerate(
    ['modified']))
CLUSTER_EVENT_FLAGS = REPLICA_EVENT_FLAGS


class Index(object):
    '''SQLite index class.'''

    def __init__(self, client, event):
        self._client = client
        self.config = client.root_config['clblob']['index']['sqlite']
        self.log = clcommon.log.get_log('clblob_index_sqlite',
            self.config['log_level'])
        if self.config['database'] is None:
            raise clcommon.config.ConfigError(_('Database must be set'))
        if self.config['tmpdir'] is not None:
            try:
                os.makedirs(self.config['tmpdir'])
            except OSError, exception:
                if exception.errno != errno.EEXIST:
                    raise
            os.environ['TMPDIR'] = self.config['tmpdir']
        self._database = None
        self._replica_ids = {}
        self._replica_names = {}
        self.log.info(_('Starting the index thread'))
        if self.config['thread']:
            self._thread = clcommon.worker.Pool(1)
        else:
            self._thread = clcommon.worker.Pool(0)
        self._thread.set_group_end(self._commit)
        self._run(self._open, event)
        self._run(self._setup_replicas, event)

    def _run(self, method, event, *args, **kwargs):
        '''Queue the index method to be run on the thread and wait for
        the result. Also mark the time it takes to get the result back.'''
        result = self._thread.start(self._run_thread, method, event, args,
            kwargs).wait()
        event.profile.mark_time('index_return:%s' % event.method)
        return result

    @staticmethod
    def _run_thread(method, event, args, kwargs):
        '''Run the index method in the thread and mark profile times.'''
        event.profile.mark_time('index_queue:%s' % event.method)
        result = method(event, *args, **kwargs)
        event.profile.mark_time('index_%s:%s' %
            (method.__name__[1:], event.method))
        return result

    def _open(self, _event):
        '''Open the database, creating tables if needed.'''
        self.log.info(_('Opening database'))
        self._database = sqlite3.connect(self.config['database'])
        self._database.execute('PRAGMA synchronous=%d' % self.config['sync'])
        self.log.info(_('Creating tables if they do not already exist'))

        self._database.execute(
            '''CREATE TABLE IF NOT EXISTS blob (
            name VARCHAR(255) NOT NULL,
            modified INTEGER,
            deleted INTEGER NOT NULL,
            modified_deleted INTEGER NOT NULL,
            PRIMARY KEY (name))''')
        self._database.execute(
            '''CREATE INDEX IF NOT EXISTS blob_modified_modified_deleted
            ON blob (modified, modified_deleted)''')
        self._database.execute(
            '''CREATE INDEX IF NOT EXISTS blob_deleted
            ON blob (deleted)''')

        self._database.execute(
            '''CREATE TABLE IF NOT EXISTS replica (
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY (name))''')

        self._database.execute(
            '''CREATE TABLE IF NOT EXISTS replica_event (
            replica INTEGER NOT NULL,
            blob INTEGER NOT NULL,
            flags INTEGER NOT NULL)''')
        self._database.execute(
            '''CREATE INDEX IF NOT EXISTS replica_event_replica_blob_flags
            ON replica_event (replica, blob, flags)''')

        self._database.execute(
            '''CREATE TABLE IF NOT EXISTS cluster_event (
            cluster INTEGER NOT NULL,
            bucket INTEGER NOT NULL,
            blob INTEGER NOT NULL,
            flags INTEGER NOT NULL)''')
        self._database.execute(
            '''CREATE INDEX IF NOT EXISTS
            cluster_event_cluster_blob_bucket_flags
            ON cluster_event (cluster, blob, bucket, flags)''')

    def _setup_replicas(self, _event):
        '''Setup the replica table for peers, adding to it if needed.'''
        self.log.info(_('Setting up the replica table'))
        cursor = self._database.execute('SELECT rowid, name FROM replica')
        for row in cursor.fetchall():
            if row[1] not in self._client.peers:
                self.log.warning(_('Skipping old replica: %s'), row[1])
                continue
            self._replica_names[row[0]] = row[1]
            self._replica_ids[row[1]] = row[0]
        for replica in self._client.peers:
            if replica in self._replica_ids:
                continue
            rowid = self._database.execute('INSERT INTO replica VALUES (?)',
                (replica,)).lastrowid
            self._replica_names[rowid] = replica
            self._replica_ids[replica] = rowid

    def _commit(self):
        '''Commit any outstanding database changes.'''
        if self._database is not None:
            self._database.commit()

    def stop(self):
        '''Stop the database thread.'''
        self.log.info(_('Stopping the index thread'))
        self._thread.stop()

    def get(self, event):
        '''Get the info for the given event.'''
        self._run(self._get, event)

    def _get(self, event):
        '''Version of method that runs in the index thread.'''
        if event.name is None:
            info = self._database.execute(
                '''SELECT rowid, name, modified, deleted, modified_deleted
                FROM blob WHERE modified=?''',
                (event.modified,)).fetchall()
        else:
            info = self._database.execute(
                '''SELECT rowid, name, modified, deleted, modified_deleted
                FROM blob WHERE name=?''',
                (event.name.decode('utf-8'),)).fetchall()
        if len(info) == 0:
            return
        index_id, name, modified, deleted, modified_deleted = info[0]
        event.index_id = index_id
        if event.name is None:
            event.name = name.encode('utf-8')
        if modified is not None and (event.modified is None or
                modified > event.modified):
            event.modified = modified
        if event.modified_deleted is None or \
                modified_deleted > event.modified_deleted:
            event.deleted = deleted
            event.modified_deleted = modified_deleted

    def update(self, event):
        '''Update the entry if we have new data, inserting if needed.'''
        return self._run(self._update, event)

    def _update(self, event):
        '''Version of method that runs in the index thread.'''
        modified = event.modified
        modified_deleted = event.modified_deleted
        self._get(event)
        event.profile.mark_time('index_get:index_update:%s' % event.method)
        if event.index_id is None:
            event.index_id = self._database.execute(
                'INSERT INTO blob VALUES (?, ?, ?, ?)',
                (event.name.decode('utf-8'), modified, event.deleted,
                    event.modified_deleted)).lastrowid
            event.profile.mark_time('insert:index_update:%s' % event.method)
        else:
            columns = []
            values = []
            if modified == event.modified:
                columns.append('modified=?')
                values.append(modified)
            else:
                modified = None
            if modified_deleted == event.modified_deleted:
                columns.append('deleted=?')
                values.append(event.deleted)
                columns.append('modified_deleted=?')
                values.append(modified_deleted)
            if len(columns) == 0:
                return None, [], []
            self._database.execute('UPDATE blob SET %s WHERE rowid=?' %
                ', '.join(columns), tuple(values + [event.index_id]))
            event.profile.mark_time('update:index_update:%s' % event.method)
        replica_events = self._add_replica_events(event, modified)
        cluster_events = self._add_cluster_events(event, modified)
        return modified, replica_events, cluster_events

    def _add_replica_events(self, event, modified):
        '''Add any replica events for the blob event.'''
        if event.replicate not in ['all', 'cluster']:
            return []
        flags = 0
        if modified is not None:
            flags |= REPLICA_EVENT_FLAGS['modified']
        events = []
        for replica in event.replicas():
            replica_id = self._replica_ids[replica]
            rowid = clcommon.number.unique64()
            self._database.execute(
                '''INSERT INTO replica_event (rowid, replica, blob, flags)
                VALUES (?, ?, ?, ?)''',
                (rowid, replica_id, event.index_id, flags))
            events.append((replica, rowid))
        event.profile.mark_time(
            'add_replica_events:index_update:%s' % event.method)
        return events

    def delete_replica_event(self, event):
        '''Delete a replica event.'''
        return self._run(self._delete_replica_event, event)

    def _delete_replica_event(self, event):
        '''Version of method that runs in the index thread.'''
        return self._database.execute(
            'DELETE FROM replica_event WHERE rowid=?',
            (event.index_id, )).rowcount

    def get_replica_event_counts(self, event):
        '''Get a count of events for each replica.'''
        return self._run(self._get_replica_event_counts, event)

    def _get_replica_event_counts(self, _event):
        '''Version of method that runs in the index thread.'''
        cursor = self._database.execute(
            'SELECT replica, COUNT(*) FROM replica_event GROUP BY replica')
        return dict((self._replica_names[replica], count)
            for replica, count in cursor.fetchall())

    def get_replica_events(self, event, replica, count):
        '''Get a list of replica events.'''
        return self._run(self._get_replica_events, event, replica, count)

    def _get_replica_events(self, _event, replica, count):
        '''Version of method that runs in the index thread.'''
        cursor = self._database.execute(
            '''SELECT replica_event.rowid, blob.name, blob.modified,
            blob.deleted, blob.modified_deleted, replica_event.flags
            FROM replica_event JOIN blob
            WHERE replica_event.replica=? AND replica_event.blob=blob.rowid
            LIMIT ?''',
            (self._replica_ids[replica], count))
        events = []
        for event in cursor:
            event = list(event)
            if not event.pop() & REPLICA_EVENT_FLAGS['modified']:
                event[2] = None
            events.append(event)
        return events

    def _add_cluster_events(self, event, modified):
        '''Add any cluster events for the blob event.'''
        if event.replicate != 'all':
            return []
        flags = 0
        if modified is not None:
            flags |= CLUSTER_EVENT_FLAGS['modified']
        events = []
        for cluster, bucket in event.buckets().iteritems():
            if self._client.cluster == cluster:
                continue
            rowid = clcommon.number.unique64()
            self._database.execute(
                '''INSERT INTO cluster_event
                (rowid, cluster, bucket, blob, flags)
                VALUES (?, ?, ?, ?, ?)''',
                (rowid, cluster, bucket, event.index_id, flags))
            events.append((cluster, bucket, rowid))
        event.profile.mark_time(
            'add_cluster_events:index_update:%s' % event.method)
        return events

    def delete_cluster_event(self, event):
        '''Delete a cluster event.'''
        return self._run(self._delete_cluster_event, event)

    def _delete_cluster_event(self, event):
        '''Version of method that runs in the index thread.'''
        return self._database.execute(
            'DELETE FROM cluster_event WHERE rowid=?',
            (event.index_id, )).rowcount

    def get_cluster_event_counts(self, event):
        '''Get a count of events for each cluster.'''
        return self._run(self._get_cluster_event_counts, event)

    def _get_cluster_event_counts(self, _event):
        '''Version of method that runs in the index thread.'''
        cursor = self._database.execute(
            'SELECT cluster, COUNT(*) FROM cluster_event GROUP BY cluster')
        return dict(cursor.fetchall())

    def get_cluster_events(self, event, cluster, count):
        '''Get a list of cluster events.'''
        return self._run(self._get_cluster_events, event, cluster, count)

    def _get_cluster_events(self, _event, cluster, count):
        '''Version of method that runs in the index thread.'''
        cursor = self._database.execute(
            '''SELECT cluster_event.rowid, cluster_event.bucket, blob.name,
            blob.modified, blob.deleted, blob.modified_deleted,
            cluster_event.flags
            FROM cluster_event JOIN blob
            WHERE cluster_event.cluster=? AND cluster_event.blob=blob.rowid
            LIMIT ?''',
            (cluster, count))
        events = []
        for event in cursor:
            event = list(event)
            if not event.pop() & REPLICA_EVENT_FLAGS['modified']:
                event[3] = None
            events.append(event)
        return events

    def list(self, event):
        '''Get a list of blob info or checksums for the requested range.'''
        if event.modified_start is None:
            event.modified_start = self._run(self._get_min_modified, event)
        if event.modified_stop is None:
            event.modified_stop = self._run(self._get_max_modified, event)
            if event.modified_stop is not None:
                event.modified_stop += 1
        if event.modified_start is None or event.modified_stop is None:
            return []
        if not event.checksum:
            return self._run(self._list, event)
        checksums = []
        modulo = event.checksum_modulo << 32
        start = event.modified_start - (event.modified_start % modulo)
        while start < event.modified_stop:
            stop = start + modulo
            checksum = self._run(self._list_checksum, event, start, stop)
            if checksum[0] > 0:
                checksums.append([start, stop, checksum[0],
                    int(checksum[1] + checksum[2]) / 2])
            start += modulo
        return checksums

    def _list(self, event):
        '''Get blob info for all blobs in the requested range.'''
        return self._database.execute(
            '''SELECT * FROM blob WHERE modified >= ? and modified < ?
            ORDER BY modified''',
            (event.modified_start, event.modified_stop)).fetchall()

    def _list_checksum(self, _event, start, stop):
        '''Get the checksum for all blobs in the requested range.'''
        return self._database.execute(
            '''SELECT COUNT(modified), AVG(modified), AVG(modified_deleted)
            FROM blob
            WHERE modified >= ? and modified < ?''',
            (start, stop)).fetchall()[0]

    def _get_min_modified(self, _event):
        '''Get the minimum modified value.'''
        return self._database.execute(
            'SELECT MIN(modified) FROM blob').fetchall()[0][0]

    def _get_max_modified(self, _event):
        '''Get the maximum modified value.'''
        return self._database.execute(
            'SELECT MAX(modified) FROM blob').fetchall()[0][0]

    def get_expired(self, event, expiration, count):
        '''Get expired blobs.'''
        return self._run(self._get_expired, event, expiration, count)

    def _get_expired(self, _event, expiration, count):
        '''Version of method that runs in the index thread.'''
        return self._database.execute(
            '''SELECT rowid, name FROM blob
            WHERE deleted > 0 AND deleted < ? LIMIT ?''',
            (expiration, count)).fetchall()

    def delete_expired(self, event, expired):
        '''Delete expired blobs.'''
        self._run(self._delete_expired, event, expired)

    def _delete_expired(self, _event, expired):
        '''Version of method that runs in the index thread.'''
        self._database.execute('DELETE FROM blob WHERE rowid IN (%s)' %
            ','.join(str(rowid) for rowid, _name in expired))

    @staticmethod
    def logstatus(event, status):
        '''Mark metrics for the index.'''
        status = status['index_sqlite']
        event.profile.mark('index_sqlite_queue_size', status['queue_size'])
        event.profile.mark('index_sqlite_index_usage',
            int(100 * float(status['size']) / status['max_size']))

    def status(self, event, status):
        '''Get status info for the index.'''
        status['index_sqlite'] = {}
        status = status['index_sqlite']
        status['database'] = self.config['database']
        status['queue_size'] = self._thread.qsize()
        self._run(self._status, event, status)

    def _status(self, _event, status):
        '''Version of method that runs in the index thread.'''
        page_size = self._database.execute('PRAGMA page_size').fetchall()[0][0]
        status['page_size'] = page_size
        response = self._database.execute('PRAGMA page_count').fetchall()
        status['size'] = response[0][0] * page_size
        response = self._database.execute('PRAGMA max_page_count').fetchall()
        status['max_size'] = response[0][0] * page_size
        response = self._database.execute('PRAGMA cache_size').fetchall()
        status['cache_size'] = response[0][0] * page_size
        response = self._database.execute('PRAGMA freelist_count').fetchall()
        status['freelist_size'] = response[0][0] * page_size
