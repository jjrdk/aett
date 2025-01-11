Feature: Saga behavior

  Scenario: Event transitions saga to next state
    Given a new saga
    When an event is applied to the saga
    Then the saga transitions to the next state