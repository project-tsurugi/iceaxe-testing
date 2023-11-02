Feature: non-scenario
    @non_scenario
    Scenario: unit test of range parse
    Given prepare THE table with data: X
    When Tx1: begin occ
    And  Tx1: read A-F
    And  Tx1: read (F:H)
    And  Tx1: read [I:M]
    And  Tx1: read (N:+inf]
    And  Tx1: read [-inf-X]
    And  Tx1: abort

    @non_scenario
    Scenario: unit test of stepdefs
    Given prepare THE empty table
    When Tx1: begin occ
    And  Tx1: read A
    And  Tx1: abort
