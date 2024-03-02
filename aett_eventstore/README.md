# Æt (Aett) is an Event Store for Python

Provides a framework for managing event streams.

## Usage

To create an event stream to manage events, you can use the `EventStream` class.

```python
from aett.eventstore.EventStream import EventStream

# Create a new event stream
event_stream = EventStream.create('bucket_name', 'stream_name')

# Append an event to the stream
event_stream.add(SomeEvent())

# Load the event stream from the event store
event_stream = EventStream.load('bucket_name', 'stream_name', [A commit store instance], 0, 100)

```

The example above is high level and does not include the details of the commit store. The commit store is a class that
implements the `CommitStore` interface and is used to store and retrieve events from the event store. An implementation 
is provided in the `aett.dynamodb.EventStore` module.
