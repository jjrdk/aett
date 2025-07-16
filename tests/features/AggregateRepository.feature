Feature: Aggregate repository behavior

  Scenario Outline: Async loading an aggregate from the repository
    Given a running <storage> server
    And I have a persistent async aggregate repository
    Then a specific aggregate type can be loaded async from the repository

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |
      | mysql_async    |

  Scenario Outline: Sync loading an aggregate from the repository
    Given a running <storage> server
    And I have a persistent aggregate repository
    Then a specific aggregate type can be loaded from the repository

    Examples:
      | storage  |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
      | mysql    |

  Scenario Outline: Async loading a modified aggregate from the repository
    Given a running <storage> server
    And I have a persistent async aggregate repository
    When an aggregate is loaded async from the repository and modified
    And an aggregate is saved async to the repository
    And loaded again async
    Then the modified aggregate is loaded from storage

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |
      | mysql_async    |

  Scenario Outline: Sync loading a modified aggregate from the repository
    Given a running <storage> server
    And I have a persistent aggregate repository
    When an aggregate is loaded from the repository and modified
    And an aggregate is saved to the repository
    And loaded again
    Then the modified aggregate is loaded from storage

    Examples:
      | storage  |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
      | mysql    |

  Scenario Outline: Async saving an aggregate to the repository
    Given a running <storage> server
    And I have a persistent async aggregate repository
    When an aggregate is loaded async from the repository and modified
    And an aggregate is saved async to the repository
    Then the modified is saved async to storage

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |
      | mysql_async    |

  Scenario Outline: Sync saving an aggregate to the repository
    Given a running <storage> server
    And I have a persistent aggregate repository
    When an aggregate is loaded from the repository and modified
    And an aggregate is saved to the repository
    Then the modified is saved to storage

    Examples:
      | storage  |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
      | mysql    |

  Scenario Outline: Load snapshots before events async
    Given a running <storage> server
    And I have a persistent async aggregate repository
    When an aggregate is created async from multiple events
    And the aggregate is snapshotted async
    And additional events are added async
    And the latest version is loaded async
    Then the aggregate is loaded from the snapshot and later events

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |
      | mysql_async    |

  Scenario Outline: Load snapshots before events
    Given a running <storage> server
    And I have a persistent aggregate repository
    When an aggregate is created from multiple events
    And the aggregate is snapshotted
    And additional events are added
    And the latest version is loaded
    Then the aggregate is loaded from the snapshot and later events

    Examples:
      | storage  |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
      | mysql    |

  Scenario Outline: Retrieving based on time async
    Given a running <storage> server
    And I have a persistent async aggregate repository
    When a series of commits is persisted async
    And a specific aggregate is loaded async at a specific time
    Then the aggregate is loaded at version 5

    Examples:
      | storage        |
      | mongo_async    |
      | postgres_async |
      | sqlite_async   |
      | mysql_async    |

  Scenario Outline: Retrieving based on time
    Given a running <storage> server
    And I have a persistent aggregate repository
    When a series of commits is persisted
    And a specific aggregate is loaded at a specific time
    Then the aggregate is loaded at version 5

    Examples:
      | storage  |
      | dynamo   |
      | inmemory |
      | mongo    |
      | postgres |
      | s3       |
      | sqlite   |
      | mysql    |

  Scenario Outline: Thousand event aggregate async
    Given a running <storage> server
    And I have a persistent async aggregate repository
    When <count> events are persisted async to an aggregate
    And loaded again async
    Then the aggregate is loaded at version <count>

    Examples:
      | storage        | count |
#      | mongo_async    | 1000  |
#      | postgres_async | 1000  |
#      | sqlite_async   | 1000  |
#      | mysql_async    | 1000  |
      | dynamodb_async | 1000  |

  Scenario Outline: Thousand event aggregate
    Given a running <storage> server
    And I have a persistent aggregate repository
    When <count> events are persisted to an aggregate
    And loaded again
    Then the aggregate is loaded at version <count>

    Examples:
      | storage  | count |
      | dynamo   | 1000  |
      | inmemory | 1000  |
      | mongo    | 1000  |
      | postgres | 1000  |
      | s3       | 100   |
      | sqlite   | 1000  |
      | mysql    | 1000  |
