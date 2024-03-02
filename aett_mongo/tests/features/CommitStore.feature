Feature: Persist commits

  Scenario: Commit event to stream
    Given I have a clean database
    When I run the setup script
    And I have a commit store
    And I commit an event to the stream
    Then the event is persisted to the store
