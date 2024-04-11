# ᚨᛖᛏᛏ (Aett) is an Event Store for Python

Provides a framework for managing and storing event streams.

## Usage

The `EventStream` class is used to manage events in a stream.
The `CommitStore` interface is used to store and retrieve events from the event store.
The `SnapshotStore` interface is used to store and retrieve snapshots from the event store.

## Domain Modeling

The `Aggregate` class is used to model domain aggregates. The `Saga` class is used to model domain sagas.

The loading and saving of aggregates is managed by the `DefaultAggregateRepository` and the `DefaultSagaRepository`
classes respectively.

Both repositories use the `CommitStore` and `SnapshotStore` interfaces to store and retrieve events and snapshots from
the persistence specific event stores.

Currently supported persistence stores are:

- DynamoDB
- MongoDB
- Postgres
- In-Memory

## Downloads

| Package                                                                      | Downloads                                                                                                 |
|------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| [aett-eventstore](https://github.com/jjrdk/aett/tree/master/aett_eventstore) | [![Downloads](https://static.pepy.tech/badge/aett-eventstore)](https://pepy.tech/project/aett-eventstore) |
| [aett-domain](https://github.com/jjrdk/aett/tree/master/aett_domain)         | [![Downloads](https://static.pepy.tech/badge/aett-domain)](https://pepy.tech/project/aett-domain)         |
| [aett-dynamodb](https://github.com/jjrdk/aett/tree/master/aett_dynamodb)     | [![Downloads](https://static.pepy.tech/badge/aett-dynamodb)](https://pepy.tech/project/aett-dynamodb)     |
| [aett-mongodb](https://github.com/jjrdk/aett/tree/master/aett_monog)         | [![Downloads](https://static.pepy.tech/badge/aett-mongodb)](https://pepy.tech/project/aett-mongodb)       |
| [aett-postgres](https://github.com/jjrdk/aett/tree/master/aett_postgres)     | [![Downloads](https://static.pepy.tech/badge/aett-postgres)](https://pepy.tech/project/aett-postgres)     |
| [aett-s3](https://github.com/jjrdk/aett/tree/master/aett_s3)                 | [![Downloads](https://static.pepy.tech/badge/aett-s3)](https://pepy.tech/project/aett-s3)                 |
| [aett-inmemory](https://github.com/jjrdk/aett/tree/master/aett_inmemory)     | [![Downloads](https://static.pepy.tech/badge/aett-inmemory)](https://pepy.tech/project/aett-inmemory)     |
