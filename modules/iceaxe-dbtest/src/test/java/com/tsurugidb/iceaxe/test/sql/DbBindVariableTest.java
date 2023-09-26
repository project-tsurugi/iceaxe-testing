package com.tsurugidb.iceaxe.test.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * bind variable test
 */
class DbBindVariableTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @Test
    void bindInsert() throws Exception {
        bindInsert(":foo", ":bar", ":zzz");
    }

    @Test
    void bindColonlessInsert1() throws Exception {
        bindInsert("f", "b", "z");
    }

    @Test
    void bindColonlessInsert2() throws Exception {
        bindInsert("foo", "bar", "zzz");
    }

    private void bindInsert(String f, String b, String z) throws IOException, InterruptedException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + f + ", " + b + ", " + z + ")";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt(trimColumn(f), TestEntity::getFoo) //
                .addLong(trimColumn(b), TestEntity::getBar) //
                .addString(trimColumn(z), TestEntity::getZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var entity = createTestEntity(SIZE);
            tm.executeAndGetCount(ps, entity);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void insertEntityWithBindVariable() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var bar = TgBindVariable.ofLong("bar");
        var zzz = TgBindVariable.ofString("zzz");
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + TgBindVariables.toSqlNames(foo, bar, zzz) + ")";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .add(foo, TestEntity::getFoo) //
                .add(bar, TestEntity::getBar) //
                .add(zzz, TestEntity::getZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var entity = createTestEntity(SIZE);
            tm.executeAndGetCount(ps, entity);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void bindCSelect() throws Exception {
        bindSelect(":bar");
    }

    @Test
    void bindColonlessSelect1() throws Exception {
        bindSelect("b");
    }

    @Test
    void bindColonlessSelect2() throws Exception {
        bindSelect("bar");
    }

    private void bindSelect(String b) throws IOException, InterruptedException {
        var bar = TgBindVariable.ofInt(trimColumn(b));
        var sql = SELECT_SQL + " where foo=" + b;
        var parameterMapping = TgParameterMapping.of(bar);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, parameterMapping, SELECT_MAPPING)) {
            int key = 2;
            var parameter = TgBindParameters.of(bar.bind(key));
            var list = tm.executeAndGetList(ps, parameter);

            if (b.equals("bar")) {
                assertEquals(SIZE, list.size());
            } else {
                assertEquals(1, list.size());
                assertEquals(createTestEntity(key), list.get(0));
            }
        }
    }

    private static String trimColumn(String s) {
        if (s.startsWith(":")) {
            return s.substring(1);
        }
        return s;
    }

    @Test
    void singleVariable_class() throws Exception {
        singleVariable(TgParameterMapping.ofSingle("foo", int.class));
    }

    @Test
    void singleVariable_type() throws Exception {
        singleVariable(TgParameterMapping.ofSingle("foo", TgDataType.INT));
    }

    private void singleVariable(TgParameterMapping<Integer> parameterMapping) throws IOException, InterruptedException {
        var sql = SELECT_SQL + " where foo = :foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, parameterMapping, SELECT_MAPPING)) {
            int key = 2;
            var list = tm.executeAndGetList(ps, key);

            assertEquals(1, list.size());
            assertEquals(createTestEntity(key), list.get(0));
        }
    }

    @Test
    void emptyBind() throws Exception {
        var parameterMapping = TgParameterMapping.of();
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, parameterMapping, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps, TgBindParameters.of());

            assertEquals(SIZE, list.size());
        }
    }

    @Test
    void emptyBindNull() throws Exception {
        var parameterMapping = TgParameterMapping.of();
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, parameterMapping, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps, null);

            assertEquals(SIZE, list.size());
        }
    }
}
