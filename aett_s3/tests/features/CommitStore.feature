Feature: Persist commits

  Scenario: Commit event to stream
    Given I have a commit store
    When I commit an event to the stream
    Then the event is persisted to the store
