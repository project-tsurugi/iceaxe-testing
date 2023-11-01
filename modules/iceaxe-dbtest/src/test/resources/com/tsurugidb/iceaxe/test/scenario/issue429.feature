Feature: case_from_429
    Scenario: issue_429_original
    Given prepare THE table with data: 
    When Tx1: begin ltx
    And  Tx1: read full
    And  Tx1: insert A
    And  Tx2: begin ltx
    And  Tx2: read B-C
    And  Tx2: upsert D
    Then Tx2: commit will waiting
    And  Tx1: commit will ok
    And  Tx2: commit-wait returns ok
