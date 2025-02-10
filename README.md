# ᚨᛖᛏᛏ (Aett) is an Event Store for Python

Provides a framework for managing and storing event streams.

## Usage

The `EventStream` class is used to manage events in a stream.
The `CommitStore` interface is used to store and retrieve events from the event store.
The `SnapshotStore` interface is used to store and retrieve snapshots from the event store.

A `CommitStore` and `SnapshotStore` implementation exists for each type of storage class. Use the appropriate module 
for your chose storage.

Example:
```python
from aett.eventstore.topic_map import TopicMap
from aett.domain.conflict_detector import ConflictDetector
from aett.storage.synchronous.postgresql.commit_store import CommitStore

commit_store = CommitStore(
                connection_string="Your connection string",
                topic_map=TopicMap(),
                conflict_detector=ConflictDetector(),
            )
```

The `CommitStore` takes in a way to connect to the database as well as:

- a `TopicMap` to map events to their application types
-  and  `ConflictDetector` to detect conflicts between competing commits

## Domain Modeling

The `Aggregate` class is used to model domain aggregates. The `Saga` class is used to model domain sagas.

The loading and saving of aggregates is managed by the `DefaultAggregateRepository` and the `DefaultSagaRepository`
classes respectively.

Both repositories use the `CommitStore` and `SnapshotStore` interfaces to store and retrieve events and snapshots from
the persistence specific event stores.

Currently supported persistence stores are:

- Sqlite
- DynamoDB
- MongoDB
- Postgres
- In-Memory
- S3

## Downloads

| Package                                                                     | Downloads                                                                                                |
|-----------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| [aett](https://github.com/jjrdk/aett) | [![Downloads](https://static.pepy.tech/badge/aett)](https://pepy.tech/project/aett) |

