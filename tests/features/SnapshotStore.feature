Feature: Persist snapshots

  Scenario Outline: Commit async snapshots of the aggregate
    Given a running <storage> server
    Given I have an async snapshot store
    And make an async snapshot of the stream
    Then the snapshot is persisted async to the store

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |

  Scenario Outline: Commit async snapshots of the aggregate
    Given a running <storage> server
    Given I have an snapshot store
    And make a snapshot of the stream
    Then the snapshot is persisted to the store

    Examples:
      | storage |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
      | mysql   |
