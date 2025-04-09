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
 * position function test
 */
class DbPositionTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbPositionTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTable();

        int i = 0;
        insert(i++, null);
        insert(i++, "");
        insert(i++, "abcdABCD");
        insert(i++, "ａｂｃＡＢＣやゆよゃゅょ");
        insert(i++, "\ud83d\ude0a\ud842\udfb7");

        logInitEnd(LOG, info);
    }

    private static void createTable() throws IOException, InterruptedException {
        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value varchar(40)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    private static void insert(int pk, String value) throws IOException, InterruptedException {
        var session = getSession();
        var v = TgBindVariable.ofString("value");
        var insertSql = "insert or replace into " + TEST + " values(" + pk + ", " + v + ")";
        var insertMapping = TgParameterMapping.of(v);
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                var parameter = TgBindParameters.of(v.bind(value));
                transaction.executeAndGetCount(ps, parameter);
            });
        }
    }

    @Test
    void testNull1() throws Exception {
        var sql = "select position(null in value) from " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        tm.executeAndForEach(sql, entity -> {
            Integer actual = entity.getIntOrNull(0);
            assertNull(actual);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "bc", "やゆよ", "\ud83d\ude0a", "\ud842\udfb7", "not found" })
    void test(String substring) throws Exception {
        var sql = String.format("select value, position('%s' in value) from " + TEST, substring);

        var tm = createTransactionManagerOcc(getSession());
        tm.executeAndForEach(sql, entity -> {
            Integer expected = position(substring, entity.getStringOrNull(0));
            Integer actual = entity.getIntOrNull(1);
            assertEquals(expected, actual);
        });
    }

    private Integer position(String substring, String string) {
        if (substring == null || string == null) {
            return null;
        }
        if (substring.isEmpty()) {
            return 1;
        }

        int[] a = string.codePoints().toArray();
        int[] s = substring.codePoints().toArray();
        loop: for (int i = 0; i < a.length; i++) {
            for (int j = 0; j < s.length; j++) {
                if (i + j >= a.length) {
                    continue loop;
                }
                if (a[i + j] != s[j]) {
                    continue loop;
                }
            }
            return i + 1;
        }

        return 0;
    }
}
