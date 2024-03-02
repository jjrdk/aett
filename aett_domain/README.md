# Æt (Aett) is an Event Store for Python

Aett Domain provide base classes for `aggregate` and `saga` as encapsulations of business rules and processes, respectively.

## Usage

The `Aggregate` class is abstract and a subtype aggregate would implement the external interfaces and internal behavior
and define which events are raised as a response input.

```python
from aett.domain.Domain import Aggregate
from dataclasses import dataclass
from aett.eventstore.EventStream import EventStream, Memento, DomainEvent
import datetime


@dataclass(frozen=True, kw_only=True)
class SampleEvent(DomainEvent):
    value: int


class ExampleAggregate(Aggregate[Memento]):
    def __init__(self, event_stream: EventStream, memento: Memento = None):
        self.value = 0
        super().__init__(event_stream, memento)

    def apply_memento(self, memento: Memento) -> None:
        if self.id != memento.id:
            raise ValueError("Memento id does not match aggregate id")
        self.value = memento.payload

    def get_memento(self) -> Memento:
        """
        The memento is a snapshot of the aggregate state. It is used to rehydrate the aggregate.
        
        It is backed by the Python __getstate__ and __setstate__ methods.
        """
        return Memento(id=self.id, version=self.version, payload={'key': self.value})

    def set_value(self, value: int) -> None:
        self.raise_event(
            SampleEvent(value=value, id=self.id, version=self.version,
                      timestamp=datetime.datetime.now(datetime.UTC)))

    def _apply(self, event: SampleEvent) -> None:
        """
        The apply method is a convention named method to apply the event to the aggregate. It is called from the raise_event method using multiple dispatch
        """
        self.value = event.value

```

The `saga` class is likewise abstract and a subtype saga would implement the external interfaces and internal behavior,
similar to the `aggregate` class.

```python
from aett.domain.Domain import Saga
from aett.eventstore.EventStream import DomainEvent
from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class SampleEvent(DomainEvent):
    value: int

    
class SampleSaga(Saga):
    def _apply(self, event: SampleEvent) -> None:
        pass

```