Feature: Aggregate repository behavior

  Background: Server started
    Given I have a persistent aggregate repository

  Scenario: Loading an aggregate from the repository
    Then a specific aggregate type can be loaded from the repository

  Scenario: Loading a modified aggregate from the repository
    When an aggregate is loaded from the repository and modified
    And an aggregate is saved to the repository
    And loaded again
    Then the modified aggregate is loaded from storage

  Scenario: Saving an aggregate to the repository
    When an aggregate is loaded from the repository and modified
    And an aggregate is saved to the repository
    Then the modified is saved to storage

  Scenario: Retrieving based on time
    When a series of commits is persisted
    And a specific aggregate is loaded at a specific time
    Then the aggregate is loaded at version 5

  Scenario: Thousand event aggregate
    When 1000 events are persisted to an aggregate
    And loaded again
    Then the aggregate is loaded at version 1000
