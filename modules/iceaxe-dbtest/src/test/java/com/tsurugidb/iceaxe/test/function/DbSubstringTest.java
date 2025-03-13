package com.tsurugidb.iceaxe.test.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * substring function test
 */
class DbSubstringTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSubstringTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTable();

        logInitEnd(LOG, info);
    }

    private static void createTable() throws IOException, InterruptedException {
        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value varchar(15)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @Test
    void testNull() throws Exception {
        insert(null);

        var tm = createTransactionManagerOcc(getSession());
        {
            var entity = tm.executeAndFindRecord("select substring(value from 1) from " + TEST).get();
            String result = entity.getStringOrNull(0);
            assertNull(result);
        }
        {
            var entity = tm.executeAndFindRecord("select substring(value from 1 for 1) from " + TEST).get();
            String result = entity.getStringOrNull(0);
            assertNull(result);
        }
        {
            var entity = tm.executeAndFindRecord("select substring('abc' from null) from " + TEST).get();
            String result = entity.getStringOrNull(0);
            assertNull(result);
        }
        {
            var entity = tm.executeAndFindRecord("select substring('abc' from null for 1) from " + TEST).get();
            String result = entity.getStringOrNull(0);
            assertNull(result);
        }
        {
            var entity = tm.executeAndFindRecord("select substring('abc' from 2 for null) from " + TEST).get();
            String result = entity.getStringOrNull(0);
            assertNull(result);
        }
        {
            var entity = tm.executeAndFindRecord("select substring('abc' from null for null) from " + TEST).get();
            String result = entity.getStringOrNull(0);
            assertNull(result);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "abcd", "あいうえお", "\ud83d\ude0a\ud842\udfb7" })
    void test(String value) throws Exception {
        insert(value);

        var tm = createTransactionManagerOcc(getSession());

        int[] codePoints = value.codePoints().toArray();
        int length = codePoints.length;
        for (int i = 0; i <= length; i++) {
            for (int j = 0; j <= length; j++) {
                var sql = String.format("select substring(value from %d for %d) from " + TEST, i, j);
                var entity = tm.executeAndFindRecord(sql).get();
                String result = entity.getStringOrNull(0);

                String expected;
                if (i == 0) {
                    expected = null;
                } else if (j == 0) {
                    expected = "";
                } else {
                    int begin = i - 1;
                    int end = begin + j;
                    if (end > length) {
                        end = length;
                    }
                    expected = new String(codePoints, begin, end - begin);
                }
                assertEquals(expected, result);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "abcd", "あいうえお", "\ud83d\ude0a\ud842\udfb7" })
    void testPlaceholder(String value) throws Exception {
        insert("dummy");

        var tm = createTransactionManagerOcc(getSession());

        int[] codePoints = value.codePoints().toArray();
        int length = codePoints.length;
        for (int i = 0; i <= length; i++) {
            for (int j = 0; j <= length; j++) {
                var sql = String.format("select substring(:v from %d for %d) from " + TEST, i, j);
                var entity = tm.executeAndFindRecord(sql, //
                        TgParameterMapping.ofSingle("v", String.class), //
                        value).get();
                String result = entity.getStringOrNull(0);

                String expected;
                if (i == 0) {
                    expected = null;
                } else if (j == 0) {
                    expected = "";
                } else {
                    int begin = i - 1;
                    int end = begin + j;
                    if (end > length) {
                        end = length;
                    }
                    expected = new String(codePoints, begin, end - begin);
                }
                assertEquals(expected, result);
            }
        }
    }

    private static void insert(String value) throws IOException, InterruptedException {
        var session = getSession();
        var v = TgBindVariable.ofString("value");
        var insertSql = "insert or replace into " + TEST + " values(1, " + v + ")";
        var insertMapping = TgParameterMapping.of(v);
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                var parameter = TgBindParameters.of(v.bind(value));
                transaction.executeAndGetCount(ps, parameter);
            });
        }
    }
}
