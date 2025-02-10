Feature: Persist commits

  Scenario Outline: Commit conflicting event to stream async
    Given a running <storage> server
    And I have a persistent async aggregate repository
    When I have an async commit store with a conflict detector
    And I commit an async event to the stream
    And I commit a conflicting event async to the stream
    Then then a conflict exception is thrown

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |

  Scenario Outline: Commit conflicting event to stream
    Given a running <storage> server
    And I have a persistent aggregate repository
    When I have an commit store with a conflict detector
    And I commit an event to the stream
    And I commit a conflicting event to the stream
    Then then a conflict exception is thrown

    Examples:
      | storage  |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |

  Scenario Outline: Commit non-conflicting event to stream async
    Given a running <storage> server
    And I have a persistent async aggregate repository
    When I have an async commit store with a conflict detector
    And I commit an async event to the stream
    And I commit a non-conflicting event async to the stream
    Then then a non-conflict exception is thrown

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |

  Scenario Outline: Commit non-conflicting event to stream
    Given a running <storage> server
    And I have a persistent aggregate repository
    When I have an commit store with a conflict detector
    And I commit an event to the stream
    And I commit a non-conflicting event to the stream
    Then then a non-conflict exception is thrown

    Examples:
      | storage  |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
