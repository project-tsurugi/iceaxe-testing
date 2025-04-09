package com.tsurugidb.iceaxe.test.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * abs function test
 */
class DbAbsTest extends DbTestTableTester {

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

    @ParameterizedTest
    @ValueSource(strings = { "int", "bigint", "real", "double", "decimal(10)", "decimal(10, 1)" })
    void test(String type) throws Exception {
        createTable(type);

        var sql = "select value, abs(value) from " + TEST;

        var tm = createTransactionManagerOcc(getSession());
        tm.executeAndForEach(sql, entity -> {
            switch (type) {
            case "int":
                assertEquals(abs(entity.getIntOrNull(0)), entity.getIntOrNull(1));
                break;
            case "bigint":
                assertEquals(abs(entity.getLongOrNull(0)), entity.getLongOrNull(1));
                break;
            case "real":
                assertEquals(abs(entity.getFloatOrNull(0)), entity.getFloatOrNull(1));
                break;
            case "double":
                assertEquals(abs(entity.getDoubleOrNull(0)), entity.getDoubleOrNull(1));
                break;
            case "decimal(10)":
            case "decimal(10, 1)":
                assertEquals(abs(entity.getDecimalOrNull(0)), entity.getDecimalOrNull(1));
                break;
            default:
                throw new AssertionError(type);
            }
        });
    }

    private Integer abs(Integer value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value);
    }

    private Long abs(Long value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value);
    }

    private Float abs(Float value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value);
    }

    private Double abs(Double value) {
        if (value == null) {
            return null;
        }
        return Math.abs(value);
    }

    private BigDecimal abs(BigDecimal value) {
        if (value == null) {
            return null;
        }
        return value.abs();
    }
}
