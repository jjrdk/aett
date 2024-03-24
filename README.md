# Æt (Aett) is an Event Store for Python

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
