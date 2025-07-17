Feature: Topic definition

  Scenario: Simple topic definition
    Given a class with a topic definition
    Then then sample_topic can be resolved from the type

  Scenario: 2 level topic definition
    Given a derived class with a topic definition
    Then then sample_topic.secondary_topic can be resolved from the type

  Scenario: 3 level topic definition
    Given a two level derived class with a topic definition
    Then then sample_topic.secondary_topic.tertiary_topic can be resolved from the type

  Scenario: Pydantic topic definition
    Given a model with a topic definition
    Then then sample_model can be resolved from the type

  Scenario: Class registration
    Given an empty topic map
    And a class with a topic definition
    When I register it with the TopicMap
    Then the topic is list of all topics
