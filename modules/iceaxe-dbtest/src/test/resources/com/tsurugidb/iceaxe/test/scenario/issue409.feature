Feature: normal_order
    Scenario Outline: 
    Given prepare THE table with data: <initial>
    When Tx1: begin ltx
    And  Tx1: read <Tx1R>
    And  Tx1: write <Tx1W>
    And  Tx2: begin ltx
    And  Tx2: read <Tx2R>
    And  Tx2: write <Tx2W>
    Then Tx1: commit will <Tx1C>
    And  Tx2: commit will <Tx2C>

    Examples:
    | initial | Tx1R | Tx1W     | Tx2R | Tx2W     | Tx1C | Tx2C |
    | ABC     | full | upsert A | B-C  | insert C | ok   | fail |
    |         | full | upsert A | B-C  | insert C | ok   | ok   |
    | ABC     | full | upsert A | B-C  | insert A | ok   | fail |
    |         | full | upsert A | B-C  | insert A | ok   | fail |
    | ABC     | full | upsert A | B-C  | delete C | ok   | ok   |
    |         | full | upsert A | B-C  | delete C | ok   | ok   |

