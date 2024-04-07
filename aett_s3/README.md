# Æt (Aett) is an Event Store for Python

[![Downloads](https://static.pepy.tech/badge/aett-postgres)](https://pepy.tech/project/aett-postgres)

Aett S3 provides the ability to store and retrieve events from an S3 bucket.

## Usage

To create an event stream to manage events, you will need to create a bucket, which will serve as the root storage.

The package also provides `CommitStore` and `SnapshotStore` classes that can be used to store and retrieve events. They
can be instantiated by providing a Postgres database connection and specifying the configured table name.

```python
from aett.s3 import CommitStore, SnapshotStore, S3Config
from aett.eventstore import TopicMap

config = S3Config(bucket='test',
                  endpoint_url='http://localhost:9000',
                  use_tls=False,
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin')
commit_store = CommitStore(s3_config=config, topic_map=TopicMap())
snapshot_store = SnapshotStore(s3_config=config)
```
