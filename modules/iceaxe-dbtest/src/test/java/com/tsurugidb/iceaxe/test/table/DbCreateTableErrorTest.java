package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create table error test
 */
class DbCreateTableErrorTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void columnNotFoundPk() throws Exception {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(goo)" //
                + ")";
        var e = executeErrorDdl(sql);
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("compile failed with error:symbol_not_found message:\"symbol 'goo' is not found\" location:<input>:", e.getMessage());
    }

    @Test
    void duplicatePkDefinition() throws Exception {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int primary key," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo)" //
                + ")";
        var e = executeErrorDdl(sql);
        assertEqualsCode(SqlServiceCode.COMPILE_EXCEPTION, e);
        assertContains("compile failed with error:primary_index_already_exists message:\"multiple primary keys are not supported\" location:<input>:", e.getMessage());
    }

    @Test
    void duplicatePk() throws Exception {
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)," //
                + "  primary key(foo, foo)" //
                + ")";
        var e = executeErrorDdl(sql);
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("compile failed with error:column_already_exists message:\"duplicate column in index definition: foo\" location:<input>:", e.getMessage());
    }

    @Test
    void duplicateColumnName() throws Exception {
        for (int i = 0; i <= 0b11; i++) {
            var sql = getDuplicateColumnSql(i);
            if (sql == null) {
                continue;
            }

            dropTestTable();
            var e = executeErrorDdl(sql);
            assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
            assertContains("compile failed with error:column_already_exists message:\"duplicate column in table definition: test.foo\" location:<input>:", e.getMessage());
        }
    }

    private static String getDuplicateColumnSql(int pk) {
        boolean pk1 = (pk & 0b1) != 0;
        boolean pk2 = (pk & 0b10) != 0;
        if (pk1 && pk2) {
            return null;
        }

        return "create table " + TEST //
                + "(" //
                + "  foo int " + (pk1 ? " primary key" : "") + "," //
                + "  foo bigint" //
                + (pk2 ? ",primary key(foo)" : "") //
                + ")";
    }

    private static TsurugiTmIOException executeErrorDdl(String sql) throws IOException {
        var tm = createTransactionManagerOcc(getSession());
        return assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeDdl(sql);
        });
    }
}
