Feature: Event serialization

    Scenario: Serializing simple message
        Given a simple message
        When serializing the simple message
        And deserializing it back
        Then should deserialize a message which contains the same values as the serialized message
          | property |
          | Id       |
          | Value    |
          | Created  |
          | Count    |
          | Contents |

    Scenario: Serializing event messages
        Given a list of event messages
        When serializing the event messages
        And deserializing them back
        Then should deserialize the same number of event messages as it serialized
        And should deserialize the complex types within the event messages

    Scenario: Serializing a list of commit headers
        Given a set of headers
        When serializing the headers
        And deserializing the headers back
        Then should deserialize the same number of headers as it serialized

    Scenario: Serializing an untyped payload on a snapshot
        Given a snapshot
        When serializing the snapshot
        And deserializing the snapshot back
        Then should correctly deserialize the untyped payload contents

    Scenario: Serializing a topic event message
        Given an event message with a topic in a commit
        When serializing the event message
        Then serializes the events as an array of strings
