Feature: Hierarchical Topic definition

  Scenario: Simple topic definition
    Given a sample class with a topic definition
    And a hierarchical topic map
    When I register it with the HierarchicalTopicMap
    Then then sample_topic can be resolved from the topic map

  Scenario: 2 level topic definition
    Given a derived class with a topic definition
    And a hierarchical topic map
    When I register it with the HierarchicalTopicMap
    Then then sample_topic.secondary_topic can be resolved from the type

  Scenario: 3 level topic definition
    Given a two level derived class with a topic definition
    And a hierarchical topic map
    When I register it with the HierarchicalTopicMap
    Then then sample_topic.secondary_topic.tertiary_topic can be resolved from the type

  Scenario: Pydantic topic definition
    Given a model with a topic definition
    And a hierarchical topic map
    And BaseModel is excepted
    When I register it with the HierarchicalTopicMap
    Then then sample_model can be resolved from the type

  Scenario: Class registration
    Given a sample class with a topic definition
    And a hierarchical topic map
    When I register it with the HierarchicalTopicMap
    Then the topic is in list of all topics
