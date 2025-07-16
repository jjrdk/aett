Feature: Topic Mapping

  Scenario: Topic annotation
    Given a class with a topic annotation
    Then the topic can be resolved from the type

  Scenario: Class registration
    Given a class with a topic annotation
    When I register it with the TopicMap
    Then the topic map can resolve the type from the topic