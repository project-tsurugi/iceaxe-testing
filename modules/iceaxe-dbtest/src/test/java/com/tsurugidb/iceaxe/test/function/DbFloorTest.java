package com.tsurugidb.iceaxe.test.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * floor function test
 */
class DbFloorTest extends DbTestTableTester {

    private static void createTable(String type) throws IOException, InterruptedException {
        dropTestTable();

        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value " + type //
                + ")";
        executeDdl(getSession(), sql);

        int i = 0;
        switch (type) {
        case "int":
        case "bigint":
        case "decimal(10)":
            insert(i++, "null");
            insert(i++, "1");
            insert(i++, "0");
            insert(i++, "-1");
            break;
        case "real":
        case "double":
        case "decimal(10, 1)":
            insert(i++, "null");
            insert(i++, "1");
            insert(i++, "0");
            insert(i++, "-1");
            insert(i++, "1.5");
            insert(i++, "0");
            insert(i++, "-1.5");
            break;
        default:
            throw new AssertionError(type);
        }
    }

    private static void insert(int pk, String value) throws IOException, InterruptedException {
        var session = getSession();
        var insertSql = "insert or replace into " + TEST + " values(" + pk + ", " + value + ")";
        try (var ps = session.createStatement(insertSql)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                transaction.executeAndGetCount(ps);
            });
        }
    }

    @Test
    void nullTest() throws Exception {
        createTable("int");

        var sql = "select floor(null) from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var list = tm.executeAndGetList(sql);
        for (var entity : list) {
            Object value = entity.getValueOrNull(0);
            assertNull(value);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "int", "bigint", "real", "double", "decimal(10)", "decimal(10, 1)" })
    void test(String type) throws Exception {
        createTable(type);

        var sql = "select value, floor(value) from " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        tm.executeAndForEach(sql, entity -> {
            switch (type) {
            case "int":
                assertEquals(floor(entity.getIntOrNull(0)), entity.getIntOrNull(1));
                break;
            case "bigint":
                assertEquals(floor(entity.getLongOrNull(0)), entity.getLongOrNull(1));
                break;
            case "real":
                assertEquals(floor(entity.getFloatOrNull(0)), entity.getFloatOrNull(1));
                break;
            case "double":
                assertEquals(floor(entity.getDoubleOrNull(0)), entity.getDoubleOrNull(1));
                break;
            case "decimal(10)":
            case "decimal(10, 1)":
                assertEquals(floor(entity.getDecimalOrNull(0)), entity.getDecimalOrNull(1));
                break;
            default:
                throw new AssertionError(type);
            }
        });
    }

    private Integer floor(Integer value) {
        if (value == null) {
            return null;
        }
        return (int) Math.floor(value);
    }

    private Long floor(Long value) {
        if (value == null) {
            return null;
        }
        return (long) Math.floor(value);
    }

    private Float floor(Float value) {
        if (value == null) {
            return null;
        }
        return (float) Math.floor(value);
    }

    private Double floor(Double value) {
        if (value == null) {
            return null;
        }
        return Math.floor(value);
    }

    private BigDecimal floor(BigDecimal value) {
        if (value == null) {
            return null;
        }
        int scale = value.scale();
        return value.setScale(0, RoundingMode.FLOOR).setScale(scale);
    }
}
