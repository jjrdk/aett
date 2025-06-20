Feature: Persist commits

  Scenario Outline: Commit event to async stream
    Given a running <storage> server
    And I have an async <storage> commit store
    When I commit an async event to the stream
    Then the event is persisted async to the store

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |

  Scenario Outline: Commit event with nested base models to async stream
    Given a running <storage> server
    And I have an async <storage> commit store
    When I commit an async event with nested base models to the stream
    Then the event is persisted async to the store
    And the nested types are preserved

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |

  Scenario Outline: Commit event to sync stream
    Given a running <storage> server
    And I have an <storage> commit store
    When I commit an event to the stream
    Then the event is persisted to the store

    Examples:
      | storage  |
      | mongo    |
      | postgres |
      | sqlite   |
      | mysql    |

  Scenario Outline: Commit event with nested base models to sync stream
    Given a running <storage> server
    And I have an <storage> commit store
    When I commit an event with nested base models to the stream
    Then the event is persisted to the store
    And the nested types are preserved

    Examples:
      | storage  |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
      | mysql    |
