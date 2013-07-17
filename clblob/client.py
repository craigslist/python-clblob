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

'''craigslist blob client module.

This module handles all requests for the blob service, and can run in
one of two possible modes:

* **proxy** - In proxy mode, the client is not responsible for a
  local replica. Therefore, it proxies all requests for blobs and replicas
  using the configuration for for clusters, buckets, and replicas that
  it knows about.

* **replica** - In replica mode, the client can perform all of the same
  duties as in proxy mode. Additionally, the client is responsible for
  handling requests for the local replica it is configured as. This
  means the client in replica mode is responsible for writing content
  to storage and metadata to the index, managing the replication stream,
  buffering events, etc. This mode is most commonly used within a server
  instance, but it can be run independently as well.

All operations working on data support file-like objects for
streaming. Full buffering never happens if possible (check with the storage
module being used), which allows you to work with very large blobs that
exceed the size of physical memory and swap space.'''

import httplib
import json
import os.path
import pkgutil
import random
import sys
import time
import traceback

import clblob.event
import clcommon.config
import clcommon.http
import clcommon.log
import clcommon.worker

DEFAULT_CONFIG = {
    'clblob': {
        'client': {
            'bucket_count_mismatch_catastrophic': True,
            'buffer_interval': 3,
            'buffer_start_delay': 0,
            'checksum_modulo': 86400,
            'churn_threshold': 0.5,
            'cluster': None,
            'cluster_event_buffer_size': 100,
            'cluster_event_retry': 1,
            'cluster_event_timeout': 5,
            'cluster_pool_size': 4,
            'clusters': [],
            'encode_name': True,
            'expiration_delay': 86400,
            'index_module': 'sqlite',
            'log_level': 'NOTSET',
            'purge_batch_size': 100,
            'purge_interval': 60,
            'purge_start_delay': 900,
            'replica': None,
            'replica_cached_connections': 10,
            'replica_event_buffer_size': 100,
            'replica_event_retry': 1,
            'replica_event_timeout': 5,
            'replica_pool_size': 2,
            'replica_retry': 10,
            'replicas': {},
            'request_timeout': 5,
            'response_buffer_size': 131072,
            'store_module': 'disk',
            'sync_blob_threshold': 1000,
            'sync_checksum_modulo_factor': 120,
            'sync_interval': None,
            'sync_start_delay': None,
            'ttl': None,
            'worker_retry': 1}}}

DEFAULT_CONFIG_FILES = [
    '/etc/clblobclient.conf',
    '~/.clblobclient.conf']

DEFAULT_CONFIG_DIRS = [
    '/etc/clblobclient.d',
    '~/.clblobclient.d']

INDEX_PATH = os.path.join(os.path.dirname(__file__), 'index')
INDEX_MODULES = dict((name, loader.find_module(name).load_module(name))
    for loader, name, _ispkg in pkgutil.iter_modules([INDEX_PATH]))
for module in INDEX_MODULES.itervalues():
    DEFAULT_CONFIG = clcommon.config.update(DEFAULT_CONFIG,
        module.DEFAULT_CONFIG)

STORE_PATH = os.path.join(os.path.dirname(__file__), 'store')
STORE_MODULES = dict((name, loader.find_module(name).load_module(name))
    for loader, name, _ispkg in pkgutil.iter_modules([STORE_PATH]))
for module in STORE_MODULES.itervalues():
    DEFAULT_CONFIG = clcommon.config.update(DEFAULT_CONFIG,
        module.DEFAULT_CONFIG)


class Client(object):
    '''This class handles all blob service requests, both local and remote
    depending on configuration. On initialization, the client executes
    the following series of actions:

    * Initializes the blob service configuration from default values,
      command-line arguments, and configuration files.
    * Checks the given config to ensure that the initialized configuration
      is sane.

    If the client is in *replica mode*, it will also:

    * Setup the configured index and store modules.
    * Checks for any uncommited blobs to ensure that any incomplete put
      requests are committed to store and index before fully starting up.
    * Creates thread pools for each worker and each cluster it talks to
      for replication.
    * Create threads for each worker: **buffer**, **purge**, **sync**.

    At this point, the client is ready to execute requests. Internally,
    requests to the blob service are translated to events (represented
    by classes in the event module) and are passed around to complete
    the request. This allows a single client instance to have multiple
    requests running at once. In fact this is preferred to take advantage
    of remote replica states and connection caching.'''

    def __init__(self, config):
        self.root_config = config
        self.config = config['clblob']['client']
        self.log = clcommon.log.get_log('clblob_client',
            self.config['log_level'])
        self.cluster = None
        self.bucket = None
        self.replica = None
        self.peers = None
        self.events = {}
        self._check_config()
        self._cluster_info = dict((cluster, {})
            for cluster in xrange(len(self.config['clusters']))
            if cluster != self.cluster)
        self._replica_info = dict((replica, dict(connections=[]))
            for replica in self.config['replicas'] if replica != self.replica)
        self._weighted_clusters = None
        if self.replica is None:
            return

        # If this client is a replica, setup storage and background tasks.
        index_module = INDEX_MODULES.get(self.config['index_module'])
        if index_module is None:
            raise clcommon.config.ConfigError(_('Unknown index module: %s') %
                self.config['index_module'])
        store_module = STORE_MODULES.get(self.config['store_module'])
        if store_module is None:
            raise clcommon.config.ConfigError(_('Unknown store module: %s') %
                self.config['store_module'])

        event = clblob.event.Admin(self, 'start')
        self._index = index_module.Index(self, event)
        self._store = store_module.Store(self, event)
        self._check_uncommitted(event)

        self._running = True
        self._start_pools(event)
        self._workers = dict((name, {
            'status': 'start delay',
            'queue': clcommon.worker.HybridQueue(),
            'thread': clcommon.worker.Pool(1, True)})
            for name in ['buffer', 'purge', 'sync'])
        for name, worker in self._workers.iteritems():
            if name == 'sync':
                default_event = lambda: clblob.event.Sync(self)
            else:
                default_event = None
            worker['thread'].start(self._watch_worker, self._worker, name,
                default_event)

    def _check_config(self):
        '''Check the config for the replica and cluster options.'''
        if len(self.config['clusters']) == 0:
            raise clcommon.config.ConfigError(_('No clusters configured'))
        seen_replicas = {}
        for cluster, buckets in enumerate(self.config['clusters']):
            if len(buckets) == 0:
                raise clcommon.config.ConfigError(
                    _('No buckets configured for cluster: %d') % cluster)
            for bucket, bucket_info in enumerate(buckets):
                self._check_bucket(cluster, bucket, bucket_info, seen_replicas)

        if self.config['replica'] is None:
            if self.config['cluster'] is None:
                return
            if self.config['cluster'] >= len(self.config['clusters']):
                raise clcommon.config.ConfigError(
                    _('Cluster option out of range: %d %d') %
                    (self.config['cluster'], len(self.config['clusters'])))
            self.cluster = self.config['cluster']
            return
        elif self.replica is None:
            raise clcommon.config.ConfigError(
                _('Replica not found in configuration: %s') %
                self.config['replica'])

    def _check_bucket(self, cluster, bucket, bucket_info, seen_replicas):
        '''Check to make sure a bucket is configured correctly.'''
        for option in ['replicas', 'write_weight']:
            if option not in bucket_info:
                raise clcommon.config.ConfigError(
                    _('No %s for cluster/bucket: %d/%d') %
                    (option, cluster, bucket))
        for replica in bucket_info['replicas']:
            if replica in seen_replicas:
                raise clcommon.config.ConfigError(
                    _('Replica can only be in one bucket: %s') % replica)
            if replica not in self.config['replicas']:
                raise clcommon.config.ConfigError(
                    _('Replica in bucket but not defined: %s') % replica)
            for option in ['ip', 'port', 'read_weight']:
                if option not in self.config['replicas'][replica]:
                    raise clcommon.config.ConfigError(
                        _('No %s for replica: %s') % (option, replica))
            if self.config['replica'] == replica:
                self.cluster = cluster
                self.bucket = bucket
                self.replica = replica
                self.peers = list(bucket_info['replicas'])
                self.peers.remove(replica)
            seen_replicas[replica] = True

    def _check_uncommitted(self, event):
        '''Check for any uncommitted blobs and let the store process them.'''
        for modified, store_id in self._store.get_uncommitted(event):
            event.name = None
            event.modified = modified
            event.store_id = store_id
            self._index.get(event)
            if event.name is None:
                self._store.abort(event)
            else:
                self._store.commit(event)

    def _start_pools(self, event):
        '''Start pools and get pending replication event counts.'''
        events = self._index.get_replica_event_counts(event)
        for replica in self.peers:
            self._replica_info[replica].update({
                'pool': clcommon.worker.Pool(self.config['replica_pool_size'],
                     True),
                'buffer_events': {},
                'events': events.get(replica, 0)})

        events = self._index.get_cluster_event_counts(event)
        for cluster, cluster_info in self._cluster_info.iteritems():
            cluster_info.update({
                'pool': clcommon.worker.Pool(self.config['cluster_pool_size'],
                     True),
                'buffer_events': {},
                'events': events.get(cluster, 0)})

    def stop(self):
        '''Stop any background tasks that may be running. This only needs
        to be called when running in replica mode to cleanly shutdown
        any background threads.'''
        if self.replica is None:
            return
        self._running = False
        for worker in self._workers.itervalues():
            worker['queue'].put(None)
            worker['thread'].stop()
        for replica in self.peers:
            self._replica_info[replica]['pool'].stop()
        for cluster_info in self._cluster_info.itervalues():
            cluster_info['pool'].stop()
        self._store.stop()
        self._index.stop()

    def buckets(self, name, encoded=False):
        '''Get the list of buckets for the given name.'''
        name = self._check_name(name)
        event = clblob.event.Put(self, name, None, encoded)
        return event.buckets().copy()

    def delete(self, name, ttl=0, deleted=None, modified_deleted=None,
            replicate='all'):
        '''Request to eventually delete a specific blob from the blob
        service at a specific time. A delete simply marks a blob as being
        acceptable for purge after a certain point in time. This does
        not guarantee the blob will be removed at this time, it merely
        provides instruction that the blob should be removed in the next
        purge operation.

        Delete can be called with either a **ttl** or a **deleted** time:

        * A **ttl** is a relative timestamp, implying "expire this blob
          TTL seconds from now. This can be a positive or negative value."
        * A **deleted** time is a specific unix epoch timestamp, implying
          "expire this blob at this unix epoch time."

        By default, a **delete** submits a TTL of 0, meaning "expire
        this as soon as possible." Every delete request is tagged with
        a unique timestamp, and the most recent one is used. This makes
        it possible for a delete request to have no effect if a later
        stamped request has already completed.

        This returns a dictionary of the current metadata for the blob
        after the delete request has been processed.

        Deletes are first forwarded to a replica that is responsible for
        it if needed. Once a replica for the name has it, it updates the
        index with the new delete time if it is newer. If it is newer,
        it is then queued for replication to all peers and at least one
        replica per cluster that is also responsible for it.'''
        name = self._check_name(name)
        event = clblob.event.Delete(self, name, replicate)
        event.deleted = self._make_deleted(ttl, deleted)
        modified_deleted = modified_deleted or clblob.unique_id()
        event.modified_deleted = modified_deleted
        return self._update(event)

    def get(self, name, response='data'):
        '''Get the blob data (if response='data') or blob info (if
        response='info') for the given name from the blob service. A get
        request does not touch the **index** if only getting data. This
        is done so that get requests can be as fast as possible. However,
        this behavior allows for a situation where it is possible for a
        blob to be returned even if the blob has been marked by a delete,
        is past its expiration timestamp, but has not yet been purged.'''
        name = self._check_name(name)
        event = clblob.event.Get(self, name, response)
        if not event.is_local:
            return self._forward(event)
        if response == 'data':
            return self._store.get(event)
        elif response == 'info':
            self._index.get(event)
            if event.modified is None:
                raise clblob.NotFound(_('No index entry: %s') % name)
            return event.info
        raise clblob.RequestError(_('Unknown response option: %s') % response)

    def name(self, name):
        '''Get the blob name that will be used for a given name. This
        returns an encoded name when encode_name is true in the config,
        otherwise it returns what was given.'''
        name = self._check_name(name)
        event = clblob.event.Put(self, name, None, False)
        return event.name

    def put(self, name, data, ttl=None, modified=None, deleted=None,
            modified_deleted=None, replicate='all', encoded=False):
        '''Put a blob into the blob service. The ttl and deleted parameters
        behave the same as with delete requests, so see the delete method
        for details. Every put request is tagged with a unique timestamp
        for both the data and deleted time, and the most recent one for
        each is used. This makes it possible for a put request to have
        no effect if a later stamped request has already completed.

        Put are first forwarded to a replica that is responsible for it
        if needed. Once a replica for the name has it, it stores the blob,
        updates the index with the new modified and delete time if they are
        newer. If it is not newer the blob that was written is aborted and
        no changes are made. If it is is newer then the blob is committed
        to the store and queued for replication to all peers and at least
        one replica per cluster that is also responsible for it.'''
        name = self._check_name(name)
        if isinstance(data, unicode):
            try:
                data = str(data)
            except UnicodeError:
                raise clblob.RequestError(
                    _('Unicode data values must be encoded first'))
        if not hasattr(data, 'read') and not isinstance(data, str):
            raise clblob.RequestError(_('Invalid data object: %s') %
                type(data))
        event = clblob.event.Put(self, name, replicate, encoded)
        event.data = data
        event.modified = modified or clblob.unique_id()
        event.deleted = self._make_deleted(ttl, deleted)
        event.modified_deleted = modified_deleted or event.modified
        return self._update(event)

    def replicas(self, name, encoded=False):
        '''Get the list of replicas for the given name.'''
        name = self._check_name(name)
        event = clblob.event.Put(self, name, None, encoded)
        return sorted(event.replicas())

    @staticmethod
    def _check_name(name):
        '''Make sure the name is valid.'''
        if not isinstance(name, basestring):
            raise clblob.InvalidRequest(_('Name must be a string'))
        if name == '':
            raise clblob.InvalidRequest(_('Name cannot be empty'))
        if name[0] == '_':
            raise clblob.InvalidRequest(_('Name cannot start with a _'))
        try:
            if isinstance(name, str):
                name.decode('utf-8')
            else:
                name = name.encode('utf-8')
        except UnicodeError:
            raise clblob.InvalidRequest(_('Name must be UTF-8 safe'))
        return name

    def _make_deleted(self, ttl, deleted):
        '''Make a deleted time from a ttl if needed.'''
        if deleted is not None:
            return deleted
        if ttl is None:
            ttl = self.config['ttl']
            if ttl is None:
                return 0
        return int(time.time()) + ttl

    def _update(self, event):
        '''Update blob data and info, along with queuing replication
        events for other replicas and clusters. It's possible that both
        put and delete updates are ultimately ignored (and therefore not
        needed to be replicated) if the timestamps are older than what
        is currently in the index.'''
        if not event.is_local:
            return self._forward(event)
        if event.data is not None:
            self._store.put(event)
        modified, replica_events, cluster_events = self._index.update(event)
        if event.data is not None:
            if modified is None:
                self._store.abort(event)
            else:
                self._store.commit(event)

        for replica, index_id in replica_events:
            self._replica_info[replica]['events'] += 1
            self._buffer_replica_event(replica, index_id, event.name,
                event.data, modified, event.deleted, event.modified_deleted)

        for cluster, bucket, index_id in cluster_events:
            self._cluster_info[cluster]['events'] += 1
            self._buffer_cluster_event(cluster, bucket, index_id, event.name,
                event.data, modified, event.deleted, event.modified_deleted)

        return event.info

    def buffer(self, replica=None):
        '''Queue request to buffer any pending events from the index. The
        default blob service configuration is setup to buffer and process
        replication events at a moderate pace to not overwhelm machine
        resources.  This admin command is a mechanism for allowing rapid
        and immediate buffering of outstanding events. A buffer call,
        from the console, HTTP request, or command-line spawns a request
        to queue events and place them into the buffer right away.

        The buffer command is a direct call to the _buffer method
        (which is also called periodically by a background worker), which
        looks at the index for any outstanding replication events. These
        outstanding replication events are placed into the buffer for
        flushing to other replicas or clusters.'''

        event = clblob.event.Admin(self, 'buffer', replica)
        if self.replica != event.replica:
            return self._request(event.replica, event)
        if self._workers['buffer']['queue'].qsize() == 0:
            self._workers['buffer']['queue'].put(event)
            return dict(queued=True)
        return dict(queued=False)

    def _buffer(self, event):
        '''Buffer replica and cluster events.'''
        max_buffer_events = self.config['replica_event_buffer_size']
        for replica in self.peers:
            buffer_events = len(self._replica_info[replica]['buffer_events'])
            if self._replica_info[replica]['events'] == buffer_events or \
                    buffer_events >= max_buffer_events:
                continue
            self._update_buffer_status(event,
                'get_replica_events %s' % replica)
            events = self._index.get_replica_events(event, replica,
                max_buffer_events)
            for index_id, name, modified, deleted, modified_deleted in events:
                if self._buffer_replica_event(replica, index_id, name, True,
                        modified, deleted, modified_deleted):
                    event.profile.mark('replica_events', 1)

        max_buffer_events = self.config['cluster_event_buffer_size']
        for cluster, cluster_info in self._cluster_info.iteritems():
            buffer_events = len(cluster_info['buffer_events'])
            if cluster_info['events'] == buffer_events or \
                    buffer_events >= max_buffer_events:
                continue
            self._update_buffer_status(event,
                'get_cluster_events %d' % cluster)
            events = self._index.get_cluster_events(event, cluster,
                max_buffer_events)
            for index_id, bucket, name, modified, deleted, modified_deleted \
                    in events:
                if self._buffer_cluster_event(cluster, bucket, index_id, name,
                        True, modified, deleted, modified_deleted):
                    event.profile.mark('cluster_events', 1)

        self._update_buffer_status(event, 'buffered')

    def _update_buffer_status(self, event, status):
        '''Update the buffer status.'''
        self._workers['buffer']['status'] = '%s replica=%d cluster=%d' % (
            status, event.profile.marks.get('replica_events', 0),
            event.profile.marks.get('cluster_events', 0))

    def _buffer_replica_event(self, replica, index_id, name, data, modified,
            deleted, modified_deleted):
        '''Buffer a replica event if it is not already in the buffer.'''
        replica_info = self._replica_info[replica]
        max_buffer_events = self.config['replica_event_buffer_size']
        if len(replica_info['buffer_events']) >= max_buffer_events or \
                index_id in replica_info['buffer_events']:
            return False
        if modified is None:
            event = clblob.event.Delete(self, name, 'replica')
        else:
            event = clblob.event.Put(self, name, 'replica', True)
            event.data = data
            event.modified = modified
        event.deleted = deleted
        event.modified_deleted = modified_deleted
        event.index_id = index_id
        event.replicas({replica: True})
        replica_info['buffer_events'][index_id] = event
        replica_info['pool'].start(self._watch_worker,
            self._replica_event_worker, event)
        return True

    def _buffer_cluster_event(self, cluster, bucket, index_id, name, data,
            modified, deleted, modified_deleted):
        '''Buffer a cluster event if it is not already in the buffer.'''
        cluster_info = self._cluster_info[cluster]
        max_buffer_events = self.config['cluster_event_buffer_size']
        if len(cluster_info['buffer_events']) >= max_buffer_events or \
                index_id in cluster_info['buffer_events']:
            return False
        if modified is None:
            event = clblob.event.Delete(self, name, 'cluster')
        else:
            event = clblob.event.Put(self, name, 'cluster', True)
            event.data = data
            event.modified = modified
        event.deleted = deleted
        event.modified_deleted = modified_deleted
        event.index_id = index_id
        event.replicas(self.config['clusters'][cluster][bucket]['replicas'])
        event.buckets({cluster: bucket})
        cluster_info['buffer_events'][index_id] = event
        cluster_info['pool'].start(self._watch_worker,
            self._cluster_event_worker, event)
        return True

    def _replica_event_worker(self, event):
        '''Send a replication event to a replica.'''
        event.profile.mark_time('replica_event_queue')
        replica = event.replicas().keys()[0]
        while self._running:
            try:
                self._request(replica, event)
            except clblob.RequestError:
                time.sleep(self.config['replica_event_retry'])
                event.profile.mark_time('replica_event_retry')
                continue
            deleted = self._index.delete_replica_event(event)
            self._replica_info[replica]['events'] -= deleted
            break
        del self._replica_info[replica]['buffer_events'][event.index_id]

    def _cluster_event_worker(self, event):
        '''Send a replication event to a replica in the cluster/bucket.'''
        event.profile.mark_time('cluster_event_queue')
        cluster = event.buckets().keys()[0]
        success = False
        while self._running and not success:
            for replica in self._get_best_replica_list(event):
                try:
                    self._request(replica, event)
                except clblob.RequestError:
                    continue
                deleted = self._index.delete_cluster_event(event)
                self._cluster_info[cluster]['events'] -= deleted
                success = True
                break
            if not success:
                time.sleep(self.config['cluster_event_retry'])
                event.profile.mark_time('cluster_event_retry')
        del self._cluster_info[cluster]['buffer_events'][event.index_id]

    def configcheck(self, config, replica=None, brief=False, tolerance=None):
        '''Compare given config against replica's config.'''
        event = clblob.event.ConfigCheck(self, replica)
        event.data = config
        event.brief = brief
        event.tolerance = tolerance
        if self.replica != event.replica:
            if not isinstance(config, basestring):
                event.data = json.dumps(config)
            return self._request(event.replica, event)
        return self._configcheck(event)

    def _configcheck(self, event):
        '''Run configcheck locally.'''
        config = event.data
        if isinstance(event.data, basestring):
            config = json.loads(config)
        config = clcommon.config.update(DEFAULT_CONFIG, config)
        clusters, options, replicas = self._configcheck_indexes({
            'new_config': config['clblob'],
            'old_config': self.root_config['clblob']})

        configcheck = {
            'catastrophic': [],
            'reconfig': [],
            'clusters': {},
            'options': {},
            'replicas': {}}
        self._configcheck_diff_clusters(configcheck, 'added',
            clusters['new_config'], clusters['old_config'])
        self._configcheck_diff_clusters(configcheck, 'removed',
            clusters['old_config'], clusters['new_config'])
        self._configcheck_diff_replicas(configcheck, replicas)
        self._configcheck_diff_options(configcheck, options)
        self._configcheck_bucket_count(configcheck, clusters['new_config'])
        self._configcheck_churn(configcheck, clusters['old_config'],
            clusters['new_config'])
        self._configcheck_catastrophic(configcheck, options['new_config'])

        event.profile.mark_time('configcheck')

        if event.brief is True:
            return self._configcheck_brief(configcheck, event.tolerance)
        return configcheck

    @staticmethod
    def _configcheck_indexes(configs):
        '''Build indexes used during configcheck.'''
        clusters = {}
        options = {}
        replicas = {}
        for version, config in configs.iteritems():
            clusters[version] = {}
            options[version] = {}
            for cluster, buckets in enumerate(config['client']['clusters']):
                clusters[version][cluster] = {}
                for bucket, bucket_info in enumerate(buckets):
                    clusters[version][cluster][bucket] = list(
                        bucket_info['replicas'])
            for replica, replica_details in \
                    config['client']['replicas'].iteritems():
                replicas[replica] = {version: replica_details}
            for option, value in config['client'].iteritems():
                if option in ['clusters', 'replicas']:
                    continue
                group_option = _("client.%s") % option
                options[version][group_option] = value
            for group in ['index', 'store']:
                for subgroup, subgroup_details in config[group].iteritems():
                    for option, value in subgroup_details.iteritems():
                        group_option = _("%s.%s.%s") % \
                            (group, subgroup, option)
                        options[version][group_option] = value
        return clusters, options, replicas

    @staticmethod
    def _configcheck_brief(configcheck, tolerance):
        '''Get a brief report of the configcheck output.'''
        if tolerance == 'catastrophic':
            return 'OK'
        if len(configcheck['catastrophic']) > 0:
            return '\n'.join(sorted(configcheck['catastrophic'])) + '\n'
        if tolerance == 'reconfig':
            return 'OK'
        if len(configcheck['reconfig']) > 0:
            return '\n'.join(sorted(configcheck['reconfig'])) + '\n'
        return 'OK'

    def _configcheck_bucket_count(self, configcheck, new_config):
        '''Check to see if bucket counts vary between clusters'''
        count = 0
        for buckets in new_config.itervalues():
            if count == 0:
                count = len(buckets)
                continue
            if count != len(buckets):
                configcheck['bucket_count_mismatch'] = True
                if self.config['bucket_count_mismatch_catastrophic'] is True:
                    configcheck['catastrophic'].append(
                        _('mismatch in bucket count between clusters'))
                break

    @staticmethod
    def _configcheck_catastrophic(configcheck, options):
        '''Check to see if the config change would cause significant changes
        in where blobs are currently stored.'''
        if 'removed' in configcheck['clusters']:
            removed = configcheck['clusters']['removed']
            if 'clusters' in removed and len(removed['clusters']) > 0:
                configcheck['catastrophic'].append(
                    _('%s clusters removed') % len(removed['clusters']))
            if 'buckets' in removed and len(removed['buckets']) > 0:
                configcheck['catastrophic'].append(
                    _('%s buckets removed') % len(removed['buckets']))
        if len(configcheck['reconfig']) > 0 and \
                options['client.encode_name'] is False:
            configcheck['catastrophic'].append(
                _('reconfig while client.encode_name is False'))

    def _configcheck_churn(self, configcheck, old_config, new_config):
        '''Calculate the churn rate for each bucket.'''
        configcheck['churn'] = {}
        for cluster, buckets in new_config.iteritems():
            if cluster not in old_config:
                continue
            for bucket, replicas in buckets.iteritems():
                if bucket not in old_config[cluster]:
                    continue
                total = 0.0
                added = 0.0
                for replica in replicas:
                    total += 1.0
                    if replica not in old_config[cluster][bucket]:
                        added += 1.0
                        continue
                if added == 0:
                    continue
                churn = added / total
                if cluster not in configcheck['churn']:
                    configcheck['churn'][cluster] = {}
                configcheck['churn'][cluster][bucket] = churn
                if churn >= self.config['churn_threshold']:
                    configcheck['catastrophic'].append(
                        _('churn exceeds threshold for bucket (%s,%s): %s') %
                        (cluster, bucket, churn * 100))

    @staticmethod
    def _configcheck_diff_clusters(configcheck, label, config1, config2):
        '''Get and label differences between two clusters configs.'''
        configcheck['clusters'][label] = diff = {}
        for cluster, buckets in config1.iteritems():
            if cluster not in config2:
                if 'clusters' not in diff:
                    diff['clusters'] = {}
                diff[cluster] = buckets
                configcheck['reconfig'].append(
                    _('cluster (%s) %s') % (cluster, label))
                continue
            for bucket, replicas in buckets.iteritems():
                if bucket not in config2[cluster]:
                    if 'buckets' not in diff:
                        diff['buckets'] = {}
                    if cluster not in diff:
                        diff['buckets'][cluster] = {}
                    diff['buckets'][cluster][bucket] = replicas
                    configcheck['reconfig'].append(
                        _('bucket (%s,%s) %s') % (cluster, bucket, label))
                    continue
                for replica in replicas:
                    if replica not in config2[cluster][bucket]:
                        if 'replicas' not in diff:
                            diff['replicas'] = {}
                        if cluster not in diff['replicas']:
                            diff['replicas'][cluster] = {}
                        if bucket not in diff['replicas'][cluster]:
                            diff['replicas'][cluster][bucket] = []
                        diff['replicas'][cluster][bucket].append(replica)
                        configcheck['reconfig'].append(
                            _('replica (%s,%s,%s) %s') %
                            (cluster, bucket, replica, label))

    @staticmethod
    def _configcheck_diff_dicts(dict_in):
        '''Get a diff of a two-tiered dictionary keyed by label'''
        index = {}
        out = {}
        for label, item in dict_in.iteritems():
            for key, value in item.iteritems():
                if key not in index:
                    index[key] = {}
                index[key][label] = value
        for key, item in index.iteritems():
            item_values = item.values()
            identical = True
            for value in item_values:
                if value != item_values[0]:
                    identical = False
                    break
            if identical is False:
                for label, value in item.iteritems():
                    if key not in out:
                        out[key] = {}
                    out[key][label] = value
        return out

    def _configcheck_diff_replicas(self, configcheck, replicas_index):
        '''Get and label differences between multiple replicas configs.'''
        for replica, replica_versions in replicas_index.iteritems():
            replica_diff = self._configcheck_diff_dicts(replica_versions)
            if len(replica_diff) > 0:
                configcheck['replicas'][replica] = replica_diff

    def _configcheck_diff_options(self, configcheck, options_index):
        '''Check for option differences between multiple configs.'''
        catastrophic_if_changed = ['client.cluster', 'client.encode_name',
            'client.index_module', 'client.replica', 'client.store_module',
            'index.sqlite.path_depth', 'index.sqlite.sync']
        configcheck['options'] = self._configcheck_diff_dicts(options_index)
        for option, diff in configcheck['options'].iteritems():
            change_string = _('option "%s" changed:') % option
            for label, value in diff.iteritems():
                change_string += (' %s: "%s"' % (label, value))
            if option in catastrophic_if_changed:
                configcheck['catastrophic'].append(change_string)
            else:
                configcheck['reconfig'].append(change_string)

    def list(self, replica=None, modified_start=None, modified_stop=None,
            checksum=True, checksum_modulo=None):
        '''Get a list of files, or a checksum of the list.'''
        event = clblob.event.List(self, replica)
        event.modified_start = modified_start
        event.modified_stop = modified_stop
        event.checksum = checksum
        if checksum_modulo is None:
            event.checksum_modulo = self.config['checksum_modulo']
        else:
            event.checksum_modulo = checksum_modulo
        if self.replica != event.replica:
            return self._request(event.replica, event)
        return self._index.list(event)

    def purge(self, replica=None):
        '''Queue request to delete any blobs that have expired. Each
        replica will periodically check the index of blobs and remove
        those blobs which have been marked for deletion. The frequency
        of this automatic purge is configurable at the client layer.

        In addition to this periodic process, a **purge** request can
        be sent directly to a server or client to trigger an immediate
        purge of the requested replica.

        The most practical use of this command is when a blob is desired to
        be removed as soon as possible.  To do this, the operator submits
        a DELETE request for the specified blob, and follows that with
        an immediate **purge** request to the replicas and clusters that
        house that blob.'''
        event = clblob.event.Admin(self, 'purge', replica)
        if self.replica != event.replica:
            return self._request(event.replica, event)
        if self._workers['purge']['queue'].qsize() == 0:
            self._workers['purge']['queue'].put(event)
            return dict(queued=True)
        return dict(queued=False)

    def _purge(self, event):
        '''Delete any blobs that have expired.'''
        purged = 0
        while True:
            self._workers['purge']['status'] = 'get_expired %d' % purged
            expiration = int(time.time()) - self.config['expiration_delay']
            expired = self._index.get_expired(event, expiration,
                self.config['purge_batch_size'])
            if len(expired) == 0:
                break
            for _rowid, name in expired:
                self._workers['purge']['status'] = 'delete %d' % purged
                event.name = name
                self._store.delete(event)
                purged += 1
            self._workers['purge']['status'] = 'delete_expired %d' % purged
            self._index.delete_expired(event, expired)

        self._workers['purge']['status'] = 'purged %d' % purged
        event.profile.mark('purged', purged)

    def status(self, replica=None):
        '''Get status information for the given replica. This returns a
        dictionary object of blob service information as understood by
        the client that handles the request. This call, and the resulting
        data, can be viewed on the command line or via the HTTP console
        to provide near-realtime status of known blob service clusters,
        buckets, and replicas.'''
        event = clblob.event.Admin(self, 'status', replica)
        if self.replica != event.replica:
            return self._request(event.replica, event)

        status = {
            'cluster': self.cluster,
            'bucket': self.bucket,
            'replica': self.replica,
            'events': dict((event, stats.copy())
                for event, stats in self.events.iteritems()),
            'replicas': {},
            'clusters': {}}

        for name, worker in self._workers.iteritems():
            status['%s_status' % name] = worker['status']

        for replica, replica_info in self._replica_info.iteritems():
            replica_status = {}
            if 'events' in replica_info:
                replica_status['events'] = replica_info['events']
                replica_status['buffer_events'] = len(
                    replica_info['buffer_events'])
            if 'last_failed' in replica_info:
                last_failed = int(time.time()) - replica_info['last_failed']
                replica_status['last_failed'] = last_failed
            if len(replica_status) > 0:
                status['replicas'][replica] = replica_status

        for cluster, cluster_info in self._cluster_info.iteritems():
            status['clusters'][cluster] = {
                'events': cluster_info['events'],
                'buffer_events': len(cluster_info['buffer_events'])}

        event.profile.mark_time('status')
        self._store.status(event, status)
        self._index.status(event, status)
        return status

    def sync(self, replica=None, source=None, modified_start=None,
            modified_stop=None):
        '''Queue request to sync with other peer replicas.'''
        event = clblob.event.Sync(self, replica)
        event.source = source
        event.modified_start = modified_start
        event.modified_stop = modified_stop
        if self.replica != event.replica:
            return self._request(event.replica, event)
        if self._workers['sync']['queue'].qsize() == 0:
            self._workers['sync']['queue'].put(event)
            return dict(queued=True)
        return dict(queued=False)

    def _sync(self, event):
        '''Sync with other peer replicas.'''
        if event.source is None:
            sources = self.peers
        else:
            sources = event.source.split(',')
        for event.source in sources:
            self._sync_checksums(event, event.modified_start,
                event.modified_stop)
        self._update_sync_status(event, 'synced')

    def _sync_checksums(self, event, modified_start, modified_stop,
            checksum_modulo=None):
        '''Sync the local and remote list of checksums for the given range.'''
        self._update_sync_status(event, 'local checksums')
        local_checksums = self.list(self.replica, modified_start,
            modified_stop, checksum_modulo=checksum_modulo)

        self._update_sync_status(event, 'source checksums')
        source_checksums = self.list(event.source, modified_start,
            modified_stop, checksum_modulo=checksum_modulo)

        self._update_sync_status(event, 'diff checksums')
        local = None
        for source in source_checksums:
            while local is None and len(local_checksums) > 0:
                local = local_checksums.pop(0)  # pylint: disable=E1103
                if local[0] < source[0]:
                    local = None
            if local == source:
                event.profile.mark('%s:skipped' % event.source, source[2])
                local = None
                continue
            next_checksum_modulo = (source[1] - source[0]) >> 32
            next_checksum_modulo /= self.config['sync_checksum_modulo_factor']
            if next_checksum_modulo <= 1 or \
                    source[2] <= self.config['sync_blob_threshold']:
                self._sync_blobs(event, source[0], source[1])
            else:
                self._sync_checksums(event, source[0], source[1],
                    next_checksum_modulo)
            if local is not None and local[0] == source[0]:
                local = None

    def _sync_blobs(self, event, modified_start, modified_stop):
        '''Sync the local and remote list of blobs for the given range.'''
        self._update_sync_status(event, 'local blobs')
        local_blobs = self.list(self.replica, modified_start, modified_stop,
            checksum=False)

        self._update_sync_status(event, 'source blobs')
        source_blobs = self.list(event.source, modified_start, modified_stop,
            checksum=False)

        self._update_sync_status(event, 'diff blobs')
        local = None
        for source in source_blobs:
            if local is not None and local[1] < source[1]:
                local = None
            while local is None and len(local_blobs) > 0:
                local = local_blobs.pop(0)  # pylint: disable=E1103
                if local[1] < source[1]:
                    local = None
            if local is None or local[1] != source[1]:
                self._sync_blob(event, 'put', source)
            elif source[3] != local[3]:
                self._sync_blob(event, 'delete', source)
            else:
                event.profile.mark('%s:skipped' % event.source, 1)

    def _sync_blob(self, event, method, source):
        '''Sync a remote blob to the local replica.'''
        self._update_sync_status(event, method)
        try:
            if method == 'delete':
                self.delete(source[0], deleted=source[2],
                    modified_deleted=source[3], replicate='replica')
            else:
                data = self._request(event.source,
                    clblob.event.Get(self, source[0], 'data'))
                self.put(source[0], data, modified=source[1],
                    deleted=source[2], modified_deleted=source[3],
                    replicate='replica', encoded=True)
            event.profile.mark('%s:%s' % (event.source, method), 1)
        except Exception:
            event.profile.mark('%s:failed' % event.source, 1)

    def _update_sync_status(self, event, status):
        '''Update the sync status.'''
        counts = ''
        for mark in sorted(event.profile.marks):
            for name in ['delete', 'failed', 'put', 'skipped']:
                value = event.profile.marks[mark]
                if value > 0 and mark.endswith(':%s' % name):
                    counts = '%s %s=%d' % (counts, mark, value)
        self._workers['sync']['status'] = '%s %s' % (status, counts)

    def _forward(self, event):
        '''Forward a blob event on to a replica that can handle it. If
        a request is received by a client that is in proxy-mode, or a
        request is received by a client in replica-mode for content that
        is not on the replica for, the client uses this method to send
        the request to the appropriate replica.'''
        replicas = self._get_best_replica_list(event)
        not_found = True
        for replica in replicas:
            try:
                return self._request(replica, event)
            except clblob.RequestError:
                not_found = False
                continue
            except clblob.NotFound:
                continue
        if not_found:
            raise clblob.NotFound(_('Blob not found on any replicas'))
        raise clblob.RequestError(_('Request failed to all replicas'))

    def _get_best_replica_list(self, event):
        '''Get the best set of replicas to try for a request. This puts
        recently failed replicas at the end of the list, and randomizes
        the non-failed replicas to try and prevent any hot spots.'''
        now = time.time()
        best = {}
        total_weight = 0
        failed = []
        for replica in event.replicas():
            last_failed = self._replica_info[replica].get('last_failed', 0)
            if last_failed + self.config['replica_retry'] < now:
                if event.method == 'get':
                    weight = self.config['replicas'][replica]['read_weight']
                else:
                    weight = 1
                if weight > 0:
                    best[replica] = weight
                    total_weight += weight
            else:
                failed.append((replica, last_failed))
        weighted_best = []
        while len(best) > 0:
            number = random.randrange(total_weight)
            current = 0
            for replica, weight in best.iteritems():
                if current <= number < current + weight:
                    weighted_best.append(replica)
                    total_weight -= weight
                    del best[replica]
                    break
                current += weight
        failed.sort(key=lambda item: item[1])
        return weighted_best + [replica for replica, _last_failed in failed]

    def _request(self, replica, event):
        '''Send a HTTP request to the given replica.'''
        try:
            cached = True
            while True:
                connection, cached = self._get_connection(replica,
                    event.timeout, cached)
                headers = {}
                data = self._request_data(event, headers)
                try:
                    connection.request(event.http_method, event.url, data,
                        headers)
                    event.profile.mark_time('%s:request' % replica)
                    response = connection.getresponse()
                except Exception, exception:
                    if not cached:
                        raise
                    cached = False
                    self.log.warning(
                        _('Cached replica request error: %s (%s %s)'),
                        replica, exception.__class__.__name__, exception)
                    continue
                break
            event.profile.mark_time('%s:response' % replica)
            if response.status == 200:
                content_type = response.getheader('Content-Type')
                if event.parse_response and content_type == 'application/json':
                    response = json.loads(response.read())
                    self._cache_connection(replica, connection)
                    return response
                return clcommon.http.Stream(response,
                    self.config['response_buffer_size'],
                    lambda: self._cache_connection(replica, connection))
            not_found = response.status == 404
            response = response.read()
        except Exception, exception:
            not_found = False
            response = '%s %s' % (exception.__class__.__name__, exception)
        self._replica_info[replica]['last_failed'] = int(time.time())
        response = _('Replica request error: %s (%s)') % (replica, response)
        self.log.warning(response)
        if not_found:
            raise clblob.NotFound(response)
        raise clblob.RequestError(response)

    def _get_connection(self, replica, timeout, cached=True):
        '''Get a connection object for a replica, using cache if we can.'''
        connection = None
        if cached:
            try:
                connection = self._replica_info[replica]['connections'].pop()
            except IndexError:
                pass
            if connection is not None:
                return connection, True
        replica_details = self.config['replicas'][replica]
        connection = httplib.HTTPConnection(replica_details['ip'],
            replica_details['port'], timeout=timeout)
        return connection, False

    def _cache_connection(self, replica, connection):
        '''Save a cached connection for later use if we can.'''
        replica_info = self._replica_info[replica]
        if len(replica_info['connections']) < \
                self.config['replica_cached_connections']:
            replica_info['connections'].append(connection)

    def _request_data(self, event, headers):
        '''Get a data object to use for the event request.'''
        if event.data is None:
            return None
        if event.data is True:
            data = self._store.get(event)
            headers['Content-Length'] = data.content_length
            return data
        if isinstance(event.data, basestring):
            headers['Content-Length'] = len(event.data)
        elif hasattr(event.data, 'content_length'):
            headers['Content-Length'] = event.data.content_length
        else:
            try:
                stat = os.fstat(event.data.fileno())
                headers['Content-Length'] = stat.st_size
            except (AttributeError, OSError):
                headers['Transfer-Encoding'] = 'chunked'
                return clcommon.http.Chunk(event.data)
        return event.data

    def _watch_worker(self, method, *args, **kwargs):
        '''Catch any exceptions and log for the given worker.'''
        while True:
            try:
                return method(*args, **kwargs)
            except Exception, exception:
                self.log.error(_('Uncaught exception in worker: %s (%s)'),
                    exception, ''.join(traceback.format_exc().split('\n')))
            time.sleep(self.config['worker_retry'])

    def _worker(self, name, default_event=None, method=None):
        '''Worker to run when requested or on an interval.'''
        if default_event is None:
            default_event = lambda: clblob.event.Admin(self, name)
        method = method or getattr(self, '_%s' % name)
        timeout = self.config['%s_start_delay' % name]
        while True:
            event = None
            try:
                event = self._workers[name]['queue'].get(timeout)
                if event is None:
                    return
                event.profile.mark_time('%s_queue' % name)
            except clcommon.worker.Empty:
                event = default_event()
            try:
                method(event)
            except Exception, exception:
                self._workers[name]['status'] = 'failed (%s)' % exception
                raise
            timeout = self.config['%s_interval' % name]

    @property
    def weighted_clusters(self):
        '''Return a weighted cluster list generated from the config.'''
        if self._weighted_clusters is not None:
            return self._weighted_clusters
        weighted_clusters = []
        for buckets in self.config['clusters']:
            weighted_buckets = []
            for bucket, bucket_info in enumerate(buckets):
                weighted_buckets += [bucket] * bucket_info['write_weight']
            weighted_clusters.append(weighted_buckets)
        self._weighted_clusters = weighted_clusters
        return self._weighted_clusters


def _main_setup(commands):
    '''Setup config and print help if needed for main function.'''
    config = clcommon.config.update(DEFAULT_CONFIG,
        clcommon.log.DEFAULT_CONFIG)
    config, args = clcommon.config.load(config, DEFAULT_CONFIG_FILES,
        DEFAULT_CONFIG_DIRS)
    clcommon.log.setup(config)
    try:
        client = Client(config)
    except clcommon.config.ConfigError, exception:
        print _('Config error: %s') % exception
        exit(1)
    if len(args) == 0 or args[0] not in commands:
        print _('Invalid command, please use on of:')
        print
        for command in commands:
            print clcommon.config.method_help(getattr(client, command))
        print
        client.stop()
        exit(1)
    return args, client


def _main():
    '''Run the blob client tool.'''
    commands = ['buckets', 'delete', 'get', 'name', 'put', 'replicas',
        'buffer', 'configcheck', 'list', 'purge', 'status', 'sync']
    args, client = _main_setup(commands)
    command = args.pop(0)
    method = getattr(client, command)
    method_args = []
    method_kwargs = {}
    for arg in args:
        arg = arg.split('=', 1)
        if len(arg) == 1:
            if len(method_args) == 0 and command in ['delete', 'get', 'put']:
                method_args.append(os.path.basename(arg[0]))
                if command == 'put':
                    method_args.append(open(arg[0]))
            else:
                method_args.append(clcommon.config.parse_value(arg[0]))
        else:
            method_kwargs[arg[0]] = clcommon.config.parse_value(arg[1])
    try:
        response = method(*method_args, **method_kwargs)
        if isinstance(response, (list, dict)):
            print json.dumps(response, indent=4, sort_keys=True)
        else:
            for chunk in response:
                sys.stdout.write(chunk)
    except clblob.RequestError, exception:
        print _('Request error: %s') % exception
        exit(1)
    finally:
        client.stop()


if __name__ == '__main__':
    _main()
