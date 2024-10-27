Feature: Persist snapshots

  Scenario: Commit snapshots of the aggregate
    Given I have a snapshot store
    And make a snapshot of the stream
    Then the snapshot is persisted to the store
