# Æt (Aett) is an Event Store for Python

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
