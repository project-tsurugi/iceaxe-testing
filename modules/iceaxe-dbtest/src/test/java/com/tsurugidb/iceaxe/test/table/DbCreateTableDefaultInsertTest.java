package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * create table default (insert) test
 */
class DbCreateTableDefaultInsertTest extends DbTestTableTester {

    private static final boolean CHECK_CURRENT_TIMESTAMP = false;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @ParameterizedTest
    @ValueSource(strings = { "1", "0", "1.0", "-1" /* TODO, "1+1" */ })
    void insertInt(String defaultValue) throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        String createSql = "create table " + TEST + " (" //
                + " pk int primary key," //
                + " value1 int," //
                + " value2 int default " + defaultValue //
                + ")";
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(1, 11, 111)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(2, null, null)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + "(pk) values(3)"));
//TODO  assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(4, default, default)"));

        var actualList = tm.executeAndGetList("select * from " + TEST + " order by pk");
        assertEquals(3, actualList.size());
        {
            var actual = actualList.get(0);
            assertEquals(1, actual.getInt("pk"));
            assertEquals(11, actual.getInt("value1"));
            assertEquals(111, actual.getInt("value2"));
        }
        {
            var actual = actualList.get(1);
            assertEquals(2, actual.getInt("pk"));
            assertNull(actual.getIntOrNull("value1"));
            assertNull(actual.getIntOrNull("value2"));
        }
        {
            var actual = actualList.get(2);
            assertEquals(3, actual.getInt("pk"));
            assertNull(actual.getIntOrNull("value1"));
            assertEquals(new BigDecimal(defaultValue).intValue(), actual.getInt("value2"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "1", "0", "1.0" /* TODO ,"-1", "1+1" */ })
    void insertLong(String defaultValue) throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        String createSql = "create table " + TEST + " (" //
                + " pk int primary key," //
                + " value1 bigint," //
                + " value2 bigint default " + defaultValue //
                + ")";
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(1, 11, 111)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(2, null, null)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + "(pk) values(3)"));
//TODO  assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(4, default, default)"));

        var actualList = tm.executeAndGetList("select * from " + TEST + " order by pk");
        assertEquals(3, actualList.size());
        {
            var actual = actualList.get(0);
            assertEquals(1, actual.getInt("pk"));
            assertEquals(11, actual.getLong("value1"));
            assertEquals(111, actual.getLong("value2"));
        }
        {
            var actual = actualList.get(1);
            assertEquals(2, actual.getInt("pk"));
            assertNull(actual.getLongOrNull("value1"));
            assertNull(actual.getLongOrNull("value2"));
        }
        {
            var actual = actualList.get(2);
            assertEquals(3, actual.getInt("pk"));
            assertNull(actual.getLongOrNull("value1"));
            assertEquals(new BigDecimal(defaultValue).longValue(), actual.getLong("value2"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "1", "0", "1.0" /* TODO ,"-1", "1+1" */ })
    void insertDouble(String defaultValue) throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        String createSql = "create table " + TEST + " (" //
                + " pk int primary key," //
                + " value1 double," //
                + " value2 double default " + defaultValue //
                + ")";
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(1, 11, 111)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(2, null, null)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + "(pk) values(3)"));
//TODO  assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(4, default, default)"));

        var actualList = tm.executeAndGetList("select * from " + TEST + " order by pk");
        assertEquals(3, actualList.size());
        {
            var actual = actualList.get(0);
            assertEquals(1, actual.getInt("pk"));
            assertEquals(11, actual.getDouble("value1"));
            assertEquals(111, actual.getDouble("value2"));
        }
        {
            var actual = actualList.get(1);
            assertEquals(2, actual.getInt("pk"));
            assertNull(actual.getDoubleOrNull("value1"));
            assertNull(actual.getDoubleOrNull("value2"));
        }
        {
            var actual = actualList.get(2);
            assertEquals(3, actual.getInt("pk"));
            assertNull(actual.getDoubleOrNull("value1"));
            assertEquals(new BigDecimal(defaultValue).doubleValue(), actual.getDouble("value2"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "1", "0", "1.2" /* TODO ,"-1", "1+1" */ })
    void insertDecimal(String defaultValue) throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        String createSql = "create table " + TEST + " (" //
                + " pk int primary key," //
                + " value1 decimal(5,1)," //
                + " value2 decimal(5,1) default " + defaultValue //
                + ")";
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(1, 11, 111)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(2, null, null)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + "(pk) values(3)"));
//TODO  assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(4, default, default)"));

        var actualList = tm.executeAndGetList("select * from " + TEST + " order by pk");
        assertEquals(3, actualList.size());
        {
            var actual = actualList.get(0);
            assertEquals(1, actual.getInt("pk"));
            assertEquals(new BigDecimal("11.0"), actual.getDecimal("value1"));
            assertEquals(new BigDecimal("111.0"), actual.getDecimal("value2"));
        }
        {
            var actual = actualList.get(1);
            assertEquals(2, actual.getInt("pk"));
            assertNull(actual.getDecimalOrNull("value1"));
            assertNull(actual.getDecimalOrNull("value2"));
        }
        {
            var actual = actualList.get(2);
            assertEquals(3, actual.getInt("pk"));
            assertNull(actual.getDecimalOrNull("value1"));
            assertEquals(new BigDecimal(defaultValue).setScale(1), actual.getDecimal("value2"));
        }
    }

    @Test
    void insertString() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        String createSql = "create table " + TEST + " (" //
                + " pk int primary key," //
                + " value1 varchar(10)," //
                + " value2 varchar(10) default 'abc'" //
                + ")";
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(1, '11', '111')"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(2, null, null)"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + "(pk) values(3)"));
//TODO  assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(4, default, default)"));

        var actualList = tm.executeAndGetList("select * from " + TEST + " order by pk");
        assertEquals(3, actualList.size());
        {
            var actual = actualList.get(0);
            assertEquals(1, actual.getInt("pk"));
            assertEquals("11", actual.getString("value1"));
            assertEquals("111", actual.getString("value2"));
        }
        {
            var actual = actualList.get(1);
            assertEquals(2, actual.getInt("pk"));
            assertNull(actual.getStringOrNull("value1"));
            assertNull(actual.getStringOrNull("value2"));
        }
        {
            var actual = actualList.get(2);
            assertEquals(3, actual.getInt("pk"));
            assertNull(actual.getStringOrNull("value1"));
            assertEquals("abc", actual.getString("value2"));
        }
    }

    @Test
    void currentTimestamp() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        String createSql = "create table " + TEST + " (" //
                + " pk int primary key," //
                + " local_date        date                     default current_date," //
                + " local_time        time                     default localtime," //
                + " local_date_time   timestamp                default localtimestamp," //
                + " current_date_time timestamp with time zone default current_timestamp" //
                + ")";
        tm.executeDdl(createSql);

        int size = 20;

        var nowBeforeTransaction = toZ(OffsetDateTime.now());
        var nowAfterTransaction = tm.execute(transaction -> {
            transaction.getLowTransaction();
            var now = toZ(OffsetDateTime.now());

            for (int i = 0; i < size; i++) {
                var sql = "insert into " + TEST + "(pk) values(" + i + ")";
                try (var ps = session.createStatement(sql)) {
                    transaction.executeAndGetCount(ps);
                }
                TimeUnit.MILLISECONDS.sleep(1);
            }
            return now;
        });

        var list = tm.executeAndGetList("select * from " + TEST);
        assertEquals(size, list.size());
        OffsetDateTime first = null;
        for (var actual : list) {
            if (first == null) {
                first = actual.getOffsetDateTime("current_date_time");
                if (CHECK_CURRENT_TIMESTAMP) {
                    var currentDateTime = toZ(first);
                    assertTrue(nowBeforeTransaction.compareTo(currentDateTime) <= 0 && currentDateTime.compareTo(nowAfterTransaction) <= 0);
                }
            } else {
                assertEquals(first, actual.getOffsetDateTime("current_date_time"));
            }

            assertEquals(actual.getDate("local_date_time"), actual.getDate("local_date"));
            assertEquals(actual.getTime("local_date_time"), actual.getTime("local_time"));
            assertEquals(actual.getDateTime("current_date_time"), actual.getDateTime("local_date_time"));
        }
    }

    private static OffsetDateTime toZ(OffsetDateTime date) {
        return date.withOffsetSameInstant(ZoneOffset.UTC);
    }
}
