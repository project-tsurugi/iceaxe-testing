package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.type.DbVarcharLenTest.VarcharTester;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * char test
 */
class DbCharLenTest extends DbTestTableTester {

    private static final int MAX_LENGTH = 2097132;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void createError() throws Exception {
        var session = getSession();

        var createSql = getCreateTable(MAX_LENGTH + 1);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            executeDdl(session, createSql);
        });
        assertEqualsCode(SqlServiceCode.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, e);
        assertContains("character type on column \"value\" is unsupported (invalid length)", e.getMessage());
    }

    private static String getCreateTable(int length) {
        return "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value char(" + length + ")" //
                + ")";
    }

    @ParameterizedTest
    @ValueSource(ints = { 100, MAX_LENGTH - 1, MAX_LENGTH })
    void insertUpdate(int maxLength) throws Exception {
        if (maxLength >= MAX_LENGTH - 1) { // TODO remove assume IPC
            assumeFalse(DbTestConnector.isIpc());
        }
        new CharTester(maxLength).test();
    }

    private static class CharTester extends VarcharTester {

        public CharTester(int maxLength) {
            super(maxLength);
        }

        @Override
        protected String getCreateSql() {
            return getCreateTable(maxLength);
        }

        @Override
        protected String getExpectedValue(String value) {
            return value + createString(' ', maxLength - value.length());
        }
    }
}
