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

'''craigslist blob package.

Purpose
=======

The blob service handles simple blob storage operations over HTTP and a
few other features such as TTLs for expiring blobs after some time. It is
designed with smart clients to remove any proxy nodes or blob location
lookups. It is multi-master with no single point of failure, and is
designed to be used both within a single data center and across multiple
data centers.

Blobs
=====

A *blob* is a binary data object of variable length with no assumed
character set or encoding. By default, the *blob service* stores
blobs as *files* on a filesystem, with metadata indexed in a *SQLite*
database. State for this service is in the path used to store blobs
(index file and data directories), which should be one per disk for best
performance and relibility.

However, the blob service is flexible; there is not a requirement that
blobs be written to disk. For example, the blob service could store blobs
in a memory storage system (ex. Redis) or in rows of a relational database
(ex. Mysql). This flexibility is the reason why the service is called a
*blob service* and not a *file service*.

Design
======

This is an introduction to the kinds of *storage objects* that contain
*blobs*.

Logical View
------------

Blob storage in the service can be divided into three nested components:
**clusters**, **buckets**, and **replicas**.

* A **cluster** represents a *bulk of blobs* available for serving
  to clients.
* A **bucket** represents a *fraction of the blobs* available within
  a cluster.
* A **replica** represents a *single storage device* within a bucket.
  By being a member of a bucket, a replica is expected to have *identical
  content as other replicas* in the same bucket.

A cluster contains one or more buckets, and a bucket contains one or
more replicas.

Blob View
---------

The *logical elements* that a blob belongs within are determined by the
**blob name**.

* A blob resides within **one or more clusters**. The blob is written
  to one cluster and optionally replicated to other clusters.
* Within each **cluster**, the blob is assigned to a single **bucket**.
* The blob is replicated amongst *all replicas* within that bucket.

Store View
----------

A *single storage device* is represented in the blob service as a
**replica**. Technically this does not have to be a physical device,
and can even be a memory storage system, relational database, or any
other kind of system for storing chunks of data.

By default, the *store* module of the blob service uses a user-writable
filesystem for storing blobs as files. In this model, the underlying
device can be a single spindle, ssd, SAN array, RAID array, or any other
kind of physical storage medium.

Since blobs are replicated amongst replicas, the storage capacity of a
bucket is limited by the lowest capacity replica within the bucket.

* A **storage device** is presented as a **replica**.
* A **replica** belongs within **one bucket**, never multiple buckets.
* A **bucket** belongs within **one cluster**, never multiple clusters.

Storage Configuration
=====================

This section explores basic configuration of the blob service data storage
objects (*clusters*, *buckets*, and *replicas*).

Clusters
--------

The **clusters** key of the configuration sets up the relationship of
clusters to buckets and replicas.  The following is an example block
of clusters::

    "clusters": [
        [
            {"replicas": ["blob_000", "blob_001"], "write_weight": 1},
            {"replicas": ["blob_010", "blob_011"], "write_weight": 1}
        ],
        [
            {"replicas": ["blob_100", "blob_101"], "write_weight": 1},
            {"replicas": ["blob_110", "blob_111"], "write_weight": 1}
        ]
    ]

In this structure, there are three tiers of configuration:

* The outermost layer is a **list of clusters**. In this list, *order is
  important!* The index position of each cluster object is used to identify
  that cluster.
* Each **cluster** is itself a list where, again, *order is important!*
  The index position of each item in the cluster list is used as the
  identifier for each **bucket**.
* Each **bucket** is a dictionary. In this case, each bucket has
  two key-values. The first is **replaces**, which is a list of replica
  identifiers. The second is a **write_weight** which defines what
  proportion of new writes will go to this bucket.

Replicas
--------

The **clusters** list shown above defines the relationship of clusters to
buckets and replicas. A second config object, the **replicas** dictionary,
contains configuration at the replica level. An example dictionary of
replicas to match the above example of clusters::

    "replicas": {
        "blob_000": {"ip": "10.0.0.100", "port": 10000, "read_weight": 1},
        "blob_001": {"ip": "10.0.0.101", "port": 10000, "read_weight": 1},
        "blob_010": {"ip": "10.0.0.110", "port": 10000, "read_weight": 1},
        "blob_011": {"ip": "10.0.0.111", "port": 10000, "read_weight": 1},
        "blob_100": {"ip": "10.0.1.100", "port": 10000, "read_weight": 1},
        "blob_101": {"ip": "10.0.1.101", "port": 10000, "read_weight": 1},
        "blob_110": {"ip": "10.0.1.110", "port": 10000, "read_weight": 1},
        "blob_111": {"ip": "10.0.1.111", "port": 10000, "read_weight": 1}
    }

Each **replica** is identified by its key name, with the value being
a dictionary of configuration key-value pairs. In this example, each
replica has three configuration keys.

* Each replica has its own daemon, so the **ip** and **port** keys define
  what this replica's daemon will bind to.
* The **read_weight** key is a value that sets the relative proportion
  of reads for this bucket that will directed to this replica.

Usage
=====

For details on the *client* and *server*, and what operations are
available, see the documentation for those modules. There is also
*benchmark* tool and module that can be used to test a running service.'''

# Install the _(...) function as a built-in so all other modules don't need to.
import gettext
gettext.install('clblob')

__version__ = '0'

import random
import time


class NotFound(Exception):
    '''Exception raised when a blob is not found.'''

    pass


class InvalidRequest(Exception):
    '''Exception raised when an invalid request is made.'''

    pass


class RequestError(Exception):
    '''Exception raised when an error is encountered while make a request.'''

    pass
