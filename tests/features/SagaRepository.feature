Feature: Saga repository behavior

  Scenario Outline: Loading an saga from the repository async
    Given a running <storage> server
    And I have a persistent async saga repository
    Then a specific saga type can be loaded async from the repository

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |

  Scenario Outline: Loading an saga from the repository
    Given a running <storage> server
    And I have a persistent saga repository
    Then a specific saga type can be loaded from the repository

    Examples:
      | storage  |
      #| dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |

  Scenario Outline: Loading a modified aggregate from the repository async
    Given a running <storage> server
    And I have a persistent async saga repository
    When a saga is loaded async from the repository and modified
    And the saga is saved async to the repository
    And the saga is loaded again async
    Then the modified saga is loaded from storage

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |

  Scenario Outline: Loading a modified aggregate from the repository
    Given a running <storage> server
    And I have a persistent saga repository
    When a saga is loaded from the repository and modified
    And the saga is saved to the repository
    And the saga is loaded again
    Then the modified saga is loaded from storage

    Examples:
      | storage  |
      #| dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
