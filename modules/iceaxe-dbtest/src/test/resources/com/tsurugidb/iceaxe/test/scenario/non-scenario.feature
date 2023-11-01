Feature: non-scenario
    @non_scenario
    Scenario: unit test of range parse
    Given prepare THE table with data: 
    When Tx1: begin occ
    And  Tx1: read A-F
    And  Tx1: read (F:H)
    And  Tx1: read [I:M]
    And  Tx1: read (N:+inf]
    And  Tx1: read [-inf-X]
    And  Tx1: abort
