package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * timestamp test
 */
class DbTimestampTest extends DbTestTableTester {

    private static final int SIZE = 5;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTable();
        insert(SIZE);

        logInitEnd(info);
    }

    private static void createTable() throws IOException, InterruptedException {
        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "value timestamp" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    private static void insert(int size) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert into " + TEST + " values(:pk, :value)";
        var insertMapping = TgParameterMapping.of(TgBindVariables.of().addInt("pk").addDateTime("value"));
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of().addInt("pk", i).addDateTime("value", LocalDateTime.of(2024, 5, 9, 23, 59, 1, (size - i) * 1000_000));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getLowColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, list.get(0));
        assertColumn("value", TgDataType.DATE_TIME, list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, SqlCommon.Column actual) {
        assertEquals(name, actual.getName());
        assertEquals(type.getLowDataType(), actual.getAtomType());
    }

    @ParameterizedTest
    @ValueSource(strings = { "2024-05-24T23:45:56.123456789", "0001-01-01T00:00:00",
            // TODO "0001-01-01T00:00:01", fix time zone serde
            "0001-01-01T00:00:00.000000001", "1970-01-01T00:00:00",
            // TODO "1969-12-31T00:00:01", fix time zone serde
            "9999-12-31T23:59:59.999999999", "-999999999-01-01T00:00:00", "+99999999-12-31T23:59:59.999999999" })
    void value(String s) throws Exception {
        var expected = LocalDateTime.parse(s);

        var variable = TgBindVariable.ofDateTime("value");
        var updateSql = "update " + TEST + " set value=" + variable + " where pk=1";
        var updateMapping = TgParameterMapping.of(variable);
        var updateParameter = TgBindParameters.of(variable.bind(expected));

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(updateSql, updateMapping, updateParameter);
        assertEquals(1, count);

        var actual = tm.executeAndFindRecord("select * from " + TEST + " where pk=1").get();
        assertEquals(expected, actual.getDateTime("value"));
    }

    @Test
    void bindWhereEq() throws Exception {
        var variable = TgBindVariable.ofDateTime("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var date = LocalDateTime.of(2024, 5, 9, 23, 59, 1, 2 * 1000_000);
            var parameter = TgBindParameters.of(variable.bind(date));
            var entity = tm.executeAndFindRecord(ps, parameter).get();
            assertEquals(date, entity.getDateTime("value"));
        }
    }

    @Test
    void bindWhereRange() throws Exception {
        var start = TgBindVariable.ofDateTime("start");
        var end = TgBindVariable.ofDateTime("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var parameter = TgBindParameters.of(start.bind(LocalDateTime.of(2024, 5, 9, 23, 59, 1, 2 * 1000_000)), end.bind(LocalDateTime.of(2024, 5, 9, 23, 59, 1, 3 * 1000_000)));
            var list = tm.executeAndGetList(ps, parameter);
            assertEquals(2, list.size());
            assertEquals(LocalDateTime.of(2024, 5, 9, 23, 59, 1, 2 * 1000_000), list.get(0).getDateTime("value"));
            assertEquals(LocalDateTime.of(2024, 5, 9, 23, 59, 1, 3 * 1000_000), list.get(1).getDateTime("value"));
        }
    }

    @Test
    @Disabled // TODO implicit conversion: char to timestamp
    void implicitConversion() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = '2024-05-09 23:59:01.002'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(LocalDateTime.of(2024, 5, 9, 23, 59, 1, 2 * 1000_000), entity.getDateTime("value"));
    }

    @Test
    @Disabled // TODO cast as timestamp
    void cast() throws Exception {
        var session = getSession();
        String sql = "update " + TEST + " set value = cast('2024-05-10 01:02:03.456' as timestamp)";
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(sql);
        assertEquals(SIZE, count);
    }

    @Test
    void min() throws Exception {
        var session = getSession();
        String sql = "select min(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalDateTime.class);
        var tm = createTransactionManagerOcc(session);
        LocalDateTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LocalDateTime.of(2024, 5, 9, 23, 59, 1, 1 * 1000_000), result);
    }

    @Test
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalDateTime.class);
        var tm = createTransactionManagerOcc(session);
        LocalDateTime result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(LocalDateTime.of(2024, 5, 9, 23, 59, 1, SIZE * 1000_000), result);
    }

    @Test
    void sum() throws Exception {
        var session = getSession();
        String sql = "select sum(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(LocalDateTime.class);
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndFindRecord(sql, resultMapping);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
        assertContains("function 'sum' is not found", e.getMessage());
    }
}
