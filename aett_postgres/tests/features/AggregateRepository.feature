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
