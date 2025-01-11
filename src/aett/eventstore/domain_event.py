from aett.eventstore import BaseEvent


class DomainEvent(BaseEvent):
    """
    Represents a single event which has occurred within the domain.
    """

    version: int
    """
    Gets the version of the aggregate which generated the event.
    """
