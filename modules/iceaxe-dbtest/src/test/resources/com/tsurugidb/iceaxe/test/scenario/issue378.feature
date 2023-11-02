Feature: case_from_378
    Scenario: issue378_case_1_initial
    Given prepare THE table with data: A
    When Tx1: begin ltx wp
    And  Tx1: read full
    And  Tx1: insert B
    And  Tx2: begin ltx wp
    And  Tx2: read full
    And  Tx2: insert C
    Then Tx1: commit will ok
    And  Tx2: commit will fail

    Scenario: issue378_case_1_empty_fullscan
    Given prepare THE table with data: 
    When Tx1: begin ltx wp
    And  Tx1: read full
    And  Tx1: insert B
    And  Tx2: begin ltx wp
    And  Tx2: read full
    And  Tx2: insert C
    Then Tx1: commit will ok
    And  Tx2: commit will fail

    Scenario: issue378_case_2
    Given prepare THE table with data: 
    When Tx1: begin ltx wp
    And  Tx1: read A
    And  Tx1: insert F
    And  Tx2: begin ltx wp
    And  Tx2: read full
    And  Tx2: insert A
    Then Tx1: commit will ok
    And  Tx2: commit will fail

    Scenario: issue378_case_2_rev
    Given prepare THE table with data: 
    When Tx1: begin ltx wp
    And  Tx1: read A
    And  Tx1: insert F
    And  Tx2: begin ltx wp
    And  Tx2: read full
    And  Tx2: insert A
    Then Tx2: commit will waiting
    And  Tx1: commit will ok
    And  Tx2: commit-wait returns fail

    Scenario: issue378_case_3
    Given prepare THE table with data: ABCDEF
    When Tx1: begin ltx wp
    And  Tx1: read A
    And  Tx1: insert G
    And  Tx2: begin ltx wp
    And  Tx2: read B-
    And  Tx2: update A
    Then Tx1: commit will ok
    And  Tx2: commit will fail
