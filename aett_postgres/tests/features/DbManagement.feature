Feature: Manage DB setup and teardown in a single place

  Scenario: Setup and teardown of DB
    Given I have a clean database
    And I run the setup script
    Then the database should be in a known state
    When I run the teardown script
    Then the database should be gone
    And I can disconnect from the database
