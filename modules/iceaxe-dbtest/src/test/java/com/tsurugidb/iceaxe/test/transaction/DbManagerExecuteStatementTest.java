package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.result.TgResultCount;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * TransactionManager execute statement test
 */
class DbManagerExecuteStatementTest extends DbTestTableTester {

    private static final int SIZE = 2;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    private static final String SQL;
    static {
        var entity = createTestEntity(SIZE);
        SQL = "insert into " + TEST //
                + " (" + TEST_COLUMNS + ")" //
                + " values(" + entity.getFoo() + "," + entity.getBar() + ",'" + entity.getZzz() + "')";
    }

    @Test
    void executeAndGetCount_sql() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        int result = tm.executeAndGetCount(SQL);

        assertUpdateCount(1, result);
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_setting_sql() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        int result = tm.executeAndGetCount(setting, SQL);

        assertUpdateCount(1, result);
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_sql_parameter() throws Exception {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        int result = tm.executeAndGetCount(INSERT_SQL, INSERT_MAPPING, entity);

        assertUpdateCount(1, result);
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_setting_sql_parameter() throws Exception {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        int result = tm.executeAndGetCount(setting, INSERT_SQL, INSERT_MAPPING, entity);

        assertUpdateCount(1, result);
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_ps() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createStatement(SQL)) {
            int result = tm.executeAndGetCount(ps);

            assertUpdateCount(1, result);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_setting_ps() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createStatement(SQL)) {
            int result = tm.executeAndGetCount(setting, ps);

            assertUpdateCount(1, result);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_ps_parameter() throws Exception {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            int result = tm.executeAndGetCount(ps, entity);

            assertUpdateCount(1, result);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_setting_ps_parameter() throws Exception {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            int result = tm.executeAndGetCount(setting, ps, entity);

            assertUpdateCount(1, result);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCountDetail_sql() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var result = tm.executeAndGetCountDetail(SQL);

        assertInsertCount(1, result);
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCountDetail_setting_sql() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var result = tm.executeAndGetCountDetail(setting, SQL);

        assertInsertCount(1, result);
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCountDetail_sql_parameter() throws Exception {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        var result = tm.executeAndGetCountDetail(INSERT_SQL, INSERT_MAPPING, entity);

        assertInsertCount(1, result);
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCountDetail_setting_sql_parameter() throws Exception {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        var result = tm.executeAndGetCountDetail(setting, INSERT_SQL, INSERT_MAPPING, entity);

        assertInsertCount(1, result);
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCountDetail_ps() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createStatement(SQL)) {
            var result = tm.executeAndGetCountDetail(ps);

            assertInsertCount(1, result);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCountDetail_setting_ps() throws Exception {
        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createStatement(SQL)) {
            var result = tm.executeAndGetCountDetail(setting, ps);

            assertInsertCount(1, result);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCountDetail_ps_parameter() throws Exception {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            var result = tm.executeAndGetCountDetail(ps, entity);

            assertInsertCount(1, result);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCountDetail_setting_ps_parameter() throws Exception {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
            var result = tm.executeAndGetCountDetail(setting, ps, entity);

            assertInsertCount(1, result);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    private static void assertInsertCount(int expected, TgResultCount result) {
        assertEquals(1, result.getLowCounterMap().size());
        assertEquals(expected, result.getInsertedCount());
    }
}
