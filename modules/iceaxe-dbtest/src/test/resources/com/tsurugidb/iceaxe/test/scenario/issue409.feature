Feature: issue_409
    Scenario Outline: normal_order
    Given prepare THE table with data: <initial>
    When Tx1: begin ltx wp
    And  Tx1: read <Tx1R>
    And  Tx1: write <Tx1W>
    And  Tx2: begin ltx wp
    And  Tx2: read <Tx2R>
    And  Tx2: write <Tx2W>
    Then Tx1: commit will <Tx1C>
    And  Tx2: commit will <Tx2C>
    #And THE table data is: <final>

    Examples:
    | initial | Tx1R | Tx1W     | Tx2R | Tx2W     | Tx1C | Tx2C |
    | ABC     | full | upsert A | B-C  | insert C | ok   | fail |
    |         | full | upsert A | B-C  | insert C | ok   | ok   |
    | ABC     | full | upsert A | B-C  | insert A | ok   | fail |
    |         | full | upsert A | B-C  | insert A | ok   | fail |
    | ABC     | full | upsert A | B-C  | delete C | ok   | ok   |
    |         | full | upsert A | B-C  | delete C | ok   | ok   |

    Scenario Outline: reverse_order
    Given prepare THE table with data: <initial>
    When Tx1: begin ltx wp
    And  Tx1: read <Tx1R>
    And  Tx1: write <Tx1W>
    And  Tx2: begin ltx wp
    And  Tx2: read <Tx2R>
    And  Tx2: write <Tx2W>
    Then Tx2: commit will waiting
    And  Tx1: commit will <Tx1C>
    And  Tx2: commit-wait returns <Tx2C>
    #And THE table data is: <final>

    Examples:
    | initial | Tx1R | Tx1W     | Tx2R | Tx2W     | Tx1C | Tx2C |
    | ABC     | full | upsert A | B-C  | insert C | ok   | fail |
    |         | full | upsert A | B-C  | insert C | ok   | ok   |
    | ABC     | full | upsert A | B-C  | insert A | ok   | fail |
    |         | full | upsert A | B-C  | insert A | ok   | fail |
    | ABC     | full | upsert A | B-C  | delete C | ok   | ok   |
    |         | full | upsert A | B-C  | delete C | ok   | ok   |
    | BCD     | A-B  | insert A | C-D  | upsert C | ok   | ok   |
    |         | A-B  | insert A | C-D  | upsert C | ok   | ok   |
    | BCD     | B-C  | insert A | C-D  | upsert A | ok   | ok   |
    |         | B-C  | insert A | C-D  | upsert A | ok   | ok   |
    | BCD     | A-B  | insert A | C-D  | upsert B | ok   | ok   |
    |         | A-B  | insert A | C-D  | upsert B | ok   | ok   |
    | BCD     | A-B  | insert A | C-D  | upsert A | ok   | ok   |
    |         | A-B  | insert A | C-D  | upsert A | ok   | ok   |

