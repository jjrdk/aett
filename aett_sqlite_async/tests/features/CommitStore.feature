Feature: Persist commits

  Scenario: Commit event to stream
    Given I have a commit store
    When I commit an event to the stream
    Then the event is persisted to the store

  Scenario: Commit event with nested base models
    Given I have a commit store
    When I commit an event with nested base models to the stream
    Then the event is persisted to the store
    And the nested types are preserved