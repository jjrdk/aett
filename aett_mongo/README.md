# Æt (Aett) is an Event Store for Python

[![Downloads](https://static.pepy.tech/badge/aett-mongodb)](https://pepy.tech/project/aett-mongodb)

Aett Mongo provides the ability to store and retrieve events from a MongoDB collection.

## Usage

To create an event stream to manage events, you can use the `PersistenceManagement` class.

```python
import pymongo.database
from aett.mongodb.EventStore import PersistenceManagement

# Set up a new event store
mgmt = PersistenceManagement(pymongo.database.Database(pymongo.MongoClient('mongodb://localhost:27017/'), 'test'))
mgmt.initialize()

# Drop the store
mgmt.drop()
```

The package also provides `CommitStore` and `SnapshotStore` classes that can be used to store and retrieve events. They
can be instantiated by providing a MongoDB database connection and specifying the configured table name.

```python
from aett.mongodb.EventStore import CommitStore, SnapshotStore
import pymongo.database

snapshot_store = SnapshotStore(pymongo.database.Database(pymongo.MongoClient('mongodb://localhost:27017/'), 'test'))
```
