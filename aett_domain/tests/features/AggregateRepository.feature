Feature: Aggregate repository behavior

  Scenario: Loading an aggregate from the repository
    Given an aggregate repository
    Then a specific aggregate type can be loaded from the repository

  Scenario: Loading an aggregate from the repository
    Given an aggregate repository
    Then a specific aggregate type can be loaded from the repository

  Scenario: Loading a modified aggregate from the repository
    Given an aggregate repository
    When an aggregate is loaded from the repository and modified
    And an aggregate is saved to the repository
    And loaded again
    Then the modified aggregate is loaded from storage

  Scenario: Saving an aggregate to the repository
    Given an aggregate repository
    When an aggregate is loaded from the repository and modified
    And an aggregate is saved to the repository
    Then the modified is saved to storage
