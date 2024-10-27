# Æt (Aett) is an Event Store for Python

[![Downloads](https://static.pepy.tech/badge/aett-postgres)](https://pepy.tech/project/aett-postgres)

Aett Postgres provides the ability to store and retrieve events from a Postgres.

## Usage

To create an event stream to manage events, you can use the `PersistenceManagement` class.

```python
import psycopg
from aett.postgres.EventStore import PersistenceManagement

# Set up a new event store
mgmt = PersistenceManagement(psycopg.connect("host=localhost port=5432 dbname=aett user=aett password=aett"))
mgmt.initialize()

# Drop the store
mgmt.drop()
```

The package also provides `CommitStore` and `SnapshotStore` classes that can be used to store and retrieve events. They
can be instantiated by providing a Postgres database connection and specifying the configured table name.

```python
from aett.postgres.EventStore import CommitStore, SnapshotStore
import pymongo.database

snapshot_store = SnapshotStore(psycopg.connect("host=localhost port=5432 dbname=aett user=aett password=aett"))
```
