package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

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
import com.tsurugidb.sql.proto.SqlCommon;
import com.tsurugidb.tsubakuro.kvs.KvsClient;
import com.tsurugidb.tsubakuro.kvs.RecordBuffer;

/**
 * decimal(5,1) test
 */
class DbDecimal1Test extends DbTestTableTester {

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
                + "  value decimal(5,1)" //
                + ")";
        var session = getSession();
        executeDdl(session, sql);
    }

    private static void insert(int size) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert into " + TEST + " values(:pk, :value)";
        var insertMapping = TgParameterMapping.of(TgBindVariables.of().addInt("pk").addDecimal("value"));
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                for (int i = 0; i < size; i++) {
                    var parameter = TgBindParameters.of().addInt("pk", i).addDecimal("value", value(size, i));
                    transaction.executeAndGetCount(ps, parameter);
                }
                return;
            });
        }
    }

    private static BigDecimal value(int size, int i) {
        return BigDecimal.valueOf(size - i - 1 + 0.1);
    }

    @Test
    void tableMetadata() throws Exception {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var list = metadata.getLowColumnList();
        assertEquals(2, list.size());
        assertColumn("pk", TgDataType.INT, list.get(0));
        assertColumn("value", TgDataType.DECIMAL, list.get(1));
    }

    private static void assertColumn(String name, TgDataType type, SqlCommon.Column actual) {
        assertEquals(name, actual.getName());
        assertEquals(type.getLowDataType(), actual.getAtomType());
    }

    @Test
    void insertLiteral() throws Exception {
        var tm = createTransactionManagerOcc(getSession());
        int i = SIZE + 1;
        tm.executeAndGetCount("insert into " + TEST + " values(" + (i++) + ", 1)");
        tm.executeAndGetCount("insert into " + TEST + " values(" + (i++) + ", 2.1)");
        tm.executeAndGetCount("insert into " + TEST + " values(" + (i++) + ", 3e2)");
        tm.executeAndGetCount("insert into " + TEST + " values(" + (i++) + ", '4.0')");
        int expectedSize = i - (SIZE + 1);

        var list = tm.executeAndGetList("select * from " + TEST + " where pk>" + SIZE + " order by pk");
        assertEquals(expectedSize, list.size());
        i = 0;
        assertEquals(new BigDecimal("1.0"), list.get(i++).getDecimal("value"));
        assertEquals(new BigDecimal("2.1"), list.get(i++).getDecimal("value"));
        assertEquals(new BigDecimal("300.0"), list.get(i++).getDecimal("value"));
        assertEquals(new BigDecimal("4.0"), list.get(i++).getDecimal("value"));
    }

    @Test
    void insertExpression() throws Exception {
        var tm = createTransactionManagerOcc(getSession());
        int i = SIZE + 1;
        tm.executeAndGetCount("insert into " + TEST + " values(" + (i++) + ", 1+1)");
        tm.executeAndGetCount("insert into " + TEST + " values(" + (i++) + ", 2.0+0.1)");
//      tm.executeAndGetCount("insert into " + TEST + " values(" + (i++) + ", 2e2+1e0)"); // TODO implicit cast
        tm.executeAndGetCount("insert into " + TEST + " values(" + (i++) + ", cast(4e2 + 1e0 as decimal))");
        int expectedSize = i - (SIZE + 1);

        var list = tm.executeAndGetList("select * from " + TEST + " where pk>" + SIZE + " order by pk");
        assertEquals(expectedSize, list.size());
        i = 0;
        assertEquals(new BigDecimal("2.0"), list.get(i++).getDecimal("value"));
        assertEquals(new BigDecimal("2.1"), list.get(i++).getDecimal("value"));
//      assertEquals(new BigDecimal("201.0"), list.get(i++).getDecimal("value"));
        assertEquals(new BigDecimal("401.0"), list.get(i++).getDecimal("value"));
    }

    @Test
    void bindWhereEq() throws Exception {
        var variable = TgBindVariable.ofDecimal("value");
        var sql = "select * from " + TEST + " where value=" + variable;
        var mapping = TgParameterMapping.of(variable);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var value = BigDecimal.valueOf(2.1);
            var parameter = TgBindParameters.of(variable.bind(value));
            var entity = tm.executeAndFindRecord(ps, parameter).get();
            assertEquals(value, entity.getDecimal("value"));
        }
    }

    @Test
    void bindWhereRange() throws Exception {
        var start = TgBindVariable.ofDecimal("start");
        var end = TgBindVariable.ofDecimal("end");
        var sql = "select * from " + TEST + " where " + start + "<=value and value<=" + end + " order by value";
        var mapping = TgParameterMapping.of(start, end);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, mapping)) {
            var parameter = TgBindParameters.of(start.bind(BigDecimal.valueOf(2.1)), end.bind(BigDecimal.valueOf(3.1)));
            var list = tm.executeAndGetList(ps, parameter);
            assertEquals(2, list.size());
            assertEquals(BigDecimal.valueOf(2.1), list.get(0).getDecimal("value"));
            assertEquals(BigDecimal.valueOf(3.1), list.get(1).getDecimal("value"));
        }
    }

    @Test
    void whereEq() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = 2.1";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(BigDecimal.valueOf(2.1), entity.getDecimal("value"));
    }

    @Test
    @Disabled // TODO implicit conversion: char to decimal
    void implicitConversion() throws Exception {
        var session = getSession();
        String sql = "select * from " + TEST + " where value = '2.1'";
        var tm = createTransactionManagerOcc(session);
        TsurugiResultEntity entity = tm.executeAndFindRecord(sql).get();
        assertEquals(BigDecimal.valueOf(2.1), entity.getDecimal("value"));
    }

    @Test
    void cast() throws Exception {
        var session = getSession();
        String sql = "update " + TEST + " set value = cast('7.7' as decimal(5,1))";
        var tm = createTransactionManagerOcc(session);
        int count = tm.executeAndGetCount(sql);
        assertEquals(SIZE, count);

        var list = tm.executeAndGetList("select * from " + TEST);
        assertEquals(SIZE, list.size());
        for (var actual : list) {
            assertEquals(BigDecimal.valueOf(7.7), actual.getDecimal("value"));
        }
    }

    @Test
    void min() throws Exception {
        var session = getSession();
        String sql = "select min(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(0.1), result);
    }

    @Test
    void max() throws Exception {
        var session = getSession();
        String sql = "select max(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(SIZE - 1 + 0.1), result);
    }

    @Test
    void sum() throws Exception {
        var session = getSession();
        String sql = "select sum(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(IntStream.range(0, SIZE).mapToDouble(n -> n + 0.1).sum()), result);
    }

    @Test
    void avg() throws Exception {
        var session = getSession();
        String sql = "select avg(value) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(IntStream.range(0, SIZE).mapToDouble(n -> n + 0.1).sum() / SIZE), result);
    }

    @Test
    void minCastAny() throws Exception {
        var session = getSession();
        String sql = "select min(cast(value as decimal(*,*))) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(0.1), result);
    }

    @Test
    void maxCastAny() throws Exception {
        var session = getSession();
        String sql = "select max(cast(value as decimal(*,*))) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(SIZE - 1 + 0.1), result);
    }

    @Test
    void sumCastAny() throws Exception {
        var session = getSession();
        String sql = "select sum(cast(value as decimal(*,*))) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(IntStream.range(0, SIZE).mapToDouble(n -> n + 0.1).sum()), result);
    }

    @Test
    void avgCastAny() throws Exception {
        var session = getSession();
        String sql = "select avg(cast(value as decimal(*,*))) from " + TEST;
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        BigDecimal result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(BigDecimal.valueOf(IntStream.range(0, SIZE).mapToDouble(n -> n + 0.1).sum() / SIZE), result);
    }

    @Test
    void kvsGet() throws Exception {
        var session = getSession();
        try (var client = KvsClient.attach(session.getLowSession()); // TODO Iceaxe KVS
                var txHandle = client.beginTransaction().await(10, TimeUnit.SECONDS)) {
            var key = new RecordBuffer().add("pk", 1);
            var result = client.get(txHandle, TEST, key).await(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());

            client.commit(txHandle).await(10, TimeUnit.SECONDS);

            var record = result.asRecord();
            assertEquals(1, record.getInt("pk"));
            assertEquals(value(SIZE, 1), record.getDecimal("value"));
        }
    }

    @ParameterizedTest
    @ValueSource(doubles = { 123, 123.4, 0, -1.1 })
    void kvsPut(double n) throws Exception {
        var expected = BigDecimal.valueOf(n);

        var session = getSession();
        try (var client = KvsClient.attach(session.getLowSession()); // TODO Iceaxe KVS
                var txHandle = client.beginTransaction().await(10, TimeUnit.SECONDS)) {
            var record = new RecordBuffer().add("pk", 1).add("value", expected);
            var result = client.put(txHandle, TEST, record).await(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());

            client.commit(txHandle).await(10, TimeUnit.SECONDS);
        }

        String sql = "select value from " + TEST + " where pk=1";
        var resultMapping = TgResultMapping.ofSingle(BigDecimal.class);
        var tm = createTransactionManagerOcc(session);
        var result = tm.executeAndFindRecord(sql, resultMapping).get();
        assertEquals(expected, result);
    }
}
