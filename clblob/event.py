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

'''craigslist blob event module.

This should only be used internally by the client module.'''

import hashlib

import clblob
import clcommon.anybase
import clcommon.profile


class Event(object):
    '''Base class for various events used in the client.'''

    params = []

    def __init__(self, client, method, name, timeout, http_method=None):
        self._client = client
        if method not in self._client.events:
            self._client.events[method] = dict(current=0, max=0, total=0)
        self._client.events[method]['total'] += 1
        self._client.events[method]['current'] += 1
        current = self._client.events[method]['current']
        if current > self._client.events[method]['max']:
            self._client.events[method]['max'] = current
        self.method = method
        self.name = name
        self.timeout = timeout
        self.http_method = http_method or method.upper()
        self.parse_response = True
        self.profile = clcommon.profile.Profile()
        self.data = None
        self.modified = None
        self.deleted = None
        self.modified_deleted = None
        self.index_id = None
        self.store_id = None
        self.encoded = None
        self._buckets = None
        self._replicas = None
        self._is_local = None

    def __del__(self):
        if hasattr(self, '_client'):
            self._client.events[self.method]['current'] -= 1
        if hasattr(self, 'profile') and len(self.profile.marks) > 0:
            self._client.log.info('profile %s', self.profile)

    @property
    def url(self):
        '''Make a URL for this event.'''
        url = '/%s' % self.name
        separator = '?'
        for param in self.params:
            value = getattr(self, param)
            if value is not None:
                url = '%s%s%s=%s' % (url, separator, param, value)
                separator = '&'
        return url

    def buckets(self, buckets=None):
        '''Get or set the buckets for this event.'''
        if buckets is not None:
            self._buckets = buckets
            return
        if self._buckets is not None:
            return self._buckets
        self._buckets = {}
        if self.encoded is not False and self._client.config['encode_name']:
            self._get_encoded_buckets()
        else:
            self._get_buckets()
        return self._buckets

    def _get_buckets(self):
        '''Get buckets for a name.'''
        name_hash = hashlib.md5(self.name).hexdigest()  # pylint: disable=E1101
        name_hash = int(name_hash[:8], 16)
        for cluster in xrange(len(self._client.weighted_clusters)):
            weighted_cluster = self._client.weighted_clusters[cluster]
            bucket = weighted_cluster[name_hash % len(weighted_cluster)]
            self._buckets[cluster] = bucket

    def _get_encoded_buckets(self):
        '''Get buckets for an encoded name.'''
        if clcommon.anybase.decode(self.name[0], 62) != 0:
            raise clblob.InvalidRequest(_('Name version not valid: %s') %
                self.name)
        buckets = self.name[1:].split('_', 1)[0]
        if len(buckets) % 2 != 0:
            raise clblob.InvalidRequest(_('Name bucket list corrupt: %s') %
                self.name)
        buckets = [buckets[offset:offset + 2]
            for offset in xrange(0, len(buckets), 2)]
        for cluster, bucket in enumerate(buckets):
            self._buckets[cluster] = clcommon.anybase.decode(bucket, 62)

    def replicas(self, replicas=None):
        '''Get or set the replicas for this event.'''
        if replicas is not None:
            self._replicas = replicas
            return
        if self._replicas is None:
            self._get_replicas()
        return self._replicas

    def _get_replicas(self):
        '''Get a preferred list of replicas for the given buckets. This
        will ignore replicas in other clusters if a cluster is configured,
        as well as the local replica if the client is a replica.'''
        self._replicas = {}
        self._is_local = False
        for cluster, bucket in self.buckets().iteritems():
            if self._client.cluster is None or self._client.cluster == cluster:
                if self._client.bucket == bucket:
                    self._is_local = True
                bucket = self._client.config['clusters'][cluster][bucket]
                for replica in bucket['replicas']:
                    if self._client.replica != replica:
                        self._replicas[replica] = True

    @property
    def is_local(self):
        '''Check to see if the local replica can handle this event.'''
        if self._is_local is None:
            self._get_replicas()
        return self._is_local

    @property
    def info(self):
        '''Make an info dictionary for responses.'''
        return dict(name=self.name, modified=self.modified,
            deleted=self.deleted, modified_deleted=self.modified_deleted,
            buckets=self.buckets())


class Get(Event):
    '''Event for tracking getting a blob.'''

    params = ['response']

    def __init__(self, client, name, response):
        super(Get, self).__init__(client, 'get', name,
            client.config['request_timeout'])
        self.response = response
        if response == 'data':
            self.parse_response = False


class Delete(Event):
    '''Event for tracking deleting a blob.'''

    params = ['deleted', 'modified_deleted', 'replicate']

    def __init__(self, client, name, replicate):
        super(Delete, self).__init__(client, 'delete', name,
            client.config['request_timeout'])
        self.replicate = replicate


class Put(Event):
    '''Event for tracking putting a blob.'''

    params = ['modified', 'deleted', 'modified_deleted', 'replicate',
        'encoded']

    def __init__(self, client, name, replicate, encoded):
        super(Put, self).__init__(client, 'put', name,
            client.config['request_timeout'])
        self.replicate = replicate
        self.encoded = encoded
        if encoded is False and client.config['encode_name']:
            self._encode_name()

    def _encode_name(self, version=0):
        '''Make a name encoded with clusters and buckets.'''
        encoded = [clcommon.anybase.encode(version, 62)]
        for _cluster, bucket in sorted(self.buckets().iteritems()):
            encoded.append(clcommon.anybase.encode(bucket, 62).zfill(2))
        self.name = '%s_%s' % (''.join(encoded), self.name)
        self.encoded = True


class Admin(Event):
    '''Event for tracking various admin tasks.'''

    def __init__(self, client, method, replica=None):
        replica = replica or client.replica
        if replica is None:
            raise clblob.RequestError(_('Must give replica'))
        elif replica not in client.config['replicas']:
            raise clblob.RequestError(_('Unknown replica: %s') % replica)
        super(Admin, self).__init__(client, method,
            '_%s/%s' % (method, replica), client.config['request_timeout'],
            'GET')
        self.replica = replica


class ConfigCheck(Event):
    '''Event for tracking configcheck requests.'''

    params = ['brief', 'tolerance']

    def __init__(self, client, replica=None):
        replica = replica or client.replica
        if replica is not None and replica not in client.config['replicas']:
            raise clblob.RequestError(_('Unknown replica: %s') % replica)
        super(ConfigCheck, self).__init__(client, 'configcheck',
            '_configcheck/%s' % replica, client.config['request_timeout'],
            'PUT')
        self.replica = replica
        self.brief = None
        self.tolerance = None


class List(Admin):
    '''Event for tracking list requests.'''

    params = ['modified_start', 'modified_stop', 'checksum', 'checksum_modulo']

    def __init__(self, client, replica=None):
        super(List, self).__init__(client, 'list', replica)
        self.modified_start = None
        self.modified_stop = None
        self.checksum = None
        self.checksum_modulo = None


class Sync(Admin):
    '''Event for tracking list requests.'''

    params = ['source', 'modified_start', 'modified_stop']

    def __init__(self, client, replica=None):
        super(Sync, self).__init__(client, 'sync', replica)
        self.source = None
        self.modified_start = None
        self.modified_stop = None
