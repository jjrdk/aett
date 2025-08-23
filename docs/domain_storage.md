# Domain Storage

In a domain-driven, event-sourced system, aggregates and sagas are stored and retrieved using an event store. This approach enables full traceability, auditability, and the ability to reconstruct state at any point in time.

## Storing Aggregates and Sagas

Aggregates and sagas are persisted by saving their uncommitted events to the event store. This is typically done through repository classes:

- **Aggregates**: Use `save(aggregate)` or `await save(aggregate)` on the aggregate repository. This persists all new events for the aggregate.
- **Sagas**: Use `save(saga)` or `await save(saga)` on the saga repository. This persists all new events for the saga.

**Example (synchronous):**
```python
context.repository.save(context.aggregate)
repository.save(context.saga)
```

**Example (asynchronous):**
```python
await context.repository.save(context.aggregate)
await repository.save(context.saga)
```

## Retrieving Aggregates and Sagas

### Aggregates
Aggregates can be retrieved from the event store in several ways:

- **Latest version:**
  ```python
  aggregate = context.repository.get(TestAggregate, context.stream_id)
  ```
- **By version:**
  ```python
  aggregate = context.repository.get(TestAggregate, context.stream_id, max_version=5)
  ```
- **By timestamp:**
  ```python
  aggregate = context.repository.get_to(TestAggregate, context.stream_id, max_time=datetime.datetime(2024, 1, 1))
  ```

These methods replay events from the event store up to the specified version or timestamp to reconstruct the aggregate's state.

### Sagas
Sagas are typically retrieved by their stream ID and always return the latest version:

```python
saga = context.repository.get(TestSaga, "test")
```

or asynchronously:

```python
saga = await context.repository.get(TestSaga, "test")
```

## Test Scenario Examples

- **Loading and modifying an aggregate:**
  ```python
  aggregate = context.repository.get(TestAggregate, context.stream_id)
  aggregate.set_value(10)
  context.repository.save(aggregate)
  ```

- **Verifying persistence:**
  ```python
  m = context.repository.get(TestAggregate, context.stream_id)
  assert m.value == 10
  ```

- **Loading a saga and applying a transition:**
  ```python
  saga = context.repository.get(TestSaga, "test")
  saga.transition(TestEvent(value=1, source="test", ...))
  context.repository.save(saga)
  ```

## Summary Table

| Entity      | Store Method         | Retrieve Latest | Retrieve by Version | Retrieve by Timestamp |
|-------------|---------------------|-----------------|--------------------|----------------------|
| Aggregate   | `save(aggregate)`   | `get(...)`      | `get(..., max_version=...)` | `get_to(..., max_time=...)` |
| Saga        | `save(saga)`        | `get(...)`      | Not typical        | Not typical          |

This event-sourced approach ensures that both aggregates and sagas can be reconstructed at any point in time, supporting robust business processes and full auditability.
