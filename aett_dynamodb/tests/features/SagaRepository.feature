Feature: Saga repository behavior

  Scenario: Loading an saga from the repository
    Given a saga repository
    Then a specific saga type can be loaded from the repository

  Scenario: Loading a saga from the repository
    Given a saga repository
    Then a specific saga type can be loaded from the repository

  Scenario: Loading a modified aggregate from the repository
    Given a saga repository
    When a saga is loaded from the repository and modified
    And the saga is saved to the repository
    And the saga is loaded again
    Then the modified saga is loaded from storage
