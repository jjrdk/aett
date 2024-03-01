Feature: Aggregate behavior

  Scenario: Can apply an event to an aggregate
    Given an aggregate
    When an event is applied to the aggregate
    Then the aggregate version is 1

    Scenario: Apply events from a serialized stream
      Given a deserialized stream of events
      And an aggregate
      When the events are applied to the aggregate
      Then the aggregate version is 2