Feature: Topic definition

  Scenario: Simple topic definition
    Given a sample class with a topic definition
    Then then sample_topic can be resolved from the type attribute

  Scenario: 2 level topic definition
    Given a derived class with a topic definition
    Then then secondary_topic can be resolved from the type attribute

  Scenario: 3 level topic definition
    Given a two level derived class with a topic definition
    Then then tertiary_topic can be resolved from the type attribute

  Scenario: Pydantic topic definition
    Given a model with a topic definition
    Then then sample_model can be resolved from the type attribute

  Scenario: Class registration
    Given an empty topic map
    And a sample class with a topic definition
    When I register it with the TopicMap
    Then the topic is list of all topics
