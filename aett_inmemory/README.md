# Æt (Aett) is an Event Store for Python

[![Downloads](https://static.pepy.tech/badge/aett-inmemory)](https://pepy.tech/project/aett-inmemory)

Aett In-Memory provides the ability to store and retrieve events from an in-memory storage.
This module should not be used for production. It is intended only for testsing.

## Usage

To create an event stream to manage events, you will need to create a bucket, which will serve as the root storage.

The package also provides `CommitStore` and `SnapshotStore` classes that can be used to store and retrieve events. They
can be instantiated by providing a Postgres database connection and specifying the configured table name.

```python
from aett.inmemory import CommitStore, SnapshotStore

commit_store = CommitStore()
snapshot_store = SnapshotStore()
```
