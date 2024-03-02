Feature: Saga repository behavior

  Background: Server started
    Given I have a clean database
    And I run the setup script
    And a persistent saga repository

  Scenario: Loading an saga from the repository
    Then a specific saga type can be loaded from the repository

  Scenario: Loading a modified aggregate from the repository
    When a saga is loaded from the repository and modified
    And the saga is saved to the repository
    And the saga is loaded again
    Then the modified saga is loaded from storage
