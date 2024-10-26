Feature: Persist commits

  Scenario: Commit event to stream
    Given I have a clean database
    When I run the setup script
    And I have a commit store with a conflict detector
    And I commit an event to the stream
    And I commit a conflicting event to the stream
    Then then a conflict exception is thrown

  Scenario: Commit event to stream
    Given I have a clean database
    When I run the setup script
    And I have a commit store with a conflict detector
    And I commit an event to the stream
    And I commit a non-conflicting event to the stream
    Then then a non-conflict exception is thrown
