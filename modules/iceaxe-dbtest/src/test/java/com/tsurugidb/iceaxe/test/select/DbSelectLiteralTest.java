package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select literal test
 */
class DbSelectLiteralTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectLiteralTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(1);

        logInitEnd(LOG, info);
    }

    private static final String COLUMN = "c";

    @Test
    void nullLiteral() throws Exception {
        test("null", entity -> {
            assertNull(entity.getStringOrNull(COLUMN));
            assertNull(entity.getIntOrNull(COLUMN));
        });
    }

    @Test
    void longLiteral() throws Exception {
        long literal = 1;
        test(Long.toString(literal), entity -> {
            assertEquals(literal, entity.getLong(COLUMN));

            assertEquals((int) literal, entity.getInt(COLUMN));
            assertEquals(BigDecimal.valueOf(literal), entity.getDecimal(COLUMN));
        });
    }

    @Test
    void doubleLiteral() throws Exception {
        double literal = 12.3;
        test(Double.toString(literal), entity -> {
            assertEquals(literal, entity.getDouble(COLUMN));

            assertEquals(BigDecimal.valueOf(literal), entity.getDecimal(COLUMN));
        });
    }

    @Test
    void doubleLiteralError() throws Exception {
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            test("1e2", entity -> {
            });
        });
        assertEqualsCode(SqlServiceCode.SYNTAX_EXCEPTION, e);
        assertContains("parsing statement failed: missing K_FROM at 'e2' (<input>:1:8)", e.getMessage());
    }

    @Test
    void stringLiteral() throws Exception {
        String literal = "abc";
        test(String.format("'%s'", literal), entity -> {
            assertEquals(literal, entity.getString(COLUMN));
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void booleanLiteral(boolean literal) throws Exception {
        test(Boolean.toString(literal), entity -> {
            assertEquals(literal, entity.getBoolean(COLUMN));
        });
    }

    // TODO date literal

    private static void test(String literal, Consumer<TsurugiResultEntity> assertion) throws IOException, InterruptedException {
        var sql = "select " + literal + " as " + COLUMN + " from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            var entity = tm.executeAndFindRecord(ps).get();
            assertion.accept(entity);
        }
    }
}
