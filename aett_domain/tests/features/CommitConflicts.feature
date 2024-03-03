Feature: Detect conflicting commits in stream

  Scenario: Non-conflicting commits
    Given an empty conflict detector
    And a stream with the following commits
      | version | author | message |
      | 1       | Alice  | A       |
      | 2       | Bob    | B       |
    When I want to add commits in the stream
      | version | author | message |
      | 3       | Jeff   | C       |
      | 4       | Jane   | D       |
    Then I should see no conflicting commits

  Scenario: Conflicting commits
    Given a configured conflict detector
    And a stream with the following commits
      | version | author | message |
      | 1       | Alice  | A       |
      | 2       | Bob    | B       |
    When I want to add commits in the stream
      | version | author | message |
      | 2       | Jeff   | C       |
    Then I should see a commit conflict
