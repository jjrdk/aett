# Æt (Aett) is an Event Store for Python

[![Downloads](https://static.pepy.tech/badge/aett-dynamodb)](https://pepy.tech/project/aett-dynamodb)

Aett DynamoDB provides the ability to store and retrieve events from a DynamoDB table.

## Usage

To create an event stream to manage events, you can use the `PersistenceManagement` class.

```python
from aett.dynamodb.EventStore import PersistenceManagement

# Set up a new event store
mgmt = PersistenceManagement()
mgmt.initialize()

# Drop the store
mgmt.drop()
```

The package also provides `CommitStore` and `SnapshotStore` classes that can be used to store and retrieve events. They
can be instantiated by providing AWS credentials and specifying the configured table name and region. If `localhost` is
set as region, then the stores assume a server running at `http://localhost:8000`.

```python
from aett.dynamodb.EventStore import CommitStore, SnapshotStore

commit_store = CommitStore()

snapshot_store = SnapshotStore()
```
