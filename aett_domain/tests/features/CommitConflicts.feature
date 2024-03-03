Feature: Detect conflicting commits in stream

  Background: Setup
    Given a conflict detector

  Scenario: Non-conflicting commits
    Given a stream with the following commits:
      | version | author | message |
      | 1       | Alice  | A       |
      | 2       | Bob    | B       |
    When I want to add a non conflicting commits in the stream
    Then I should see no conflicting commits