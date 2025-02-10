Feature: Saga behavior

  Scenario: Event transitions saga to next state
    Given a new saga
    When an event is applied to the saga
    Then the saga transitions to the next state

  Scenario: Event emits command from saga
    Given a new saga
    When an event is applied to the saga
    Then a command is emitted from the saga
#    And can be read from the persisted event headers
