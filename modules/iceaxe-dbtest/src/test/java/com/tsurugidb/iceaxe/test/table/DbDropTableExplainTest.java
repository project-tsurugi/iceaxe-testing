package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * explain drop table test
 */
class DbDropTableExplainTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    private static final String SQL = "drop table " + TEST;

    @Test
    void drop() throws Exception {
        createTestTable();

        var session = getSession();
        var helper = session.getExplainHelper();
        var result = helper.explain(session, SQL);
        assertExplain(result);
    }

    @Test
    void dropNotExists() throws Exception {
        var session = getSession();
        var helper = session.getExplainHelper();
        var e = assertThrowsExactly(TsurugiIOException.class, () -> {
            helper.explain(session, SQL);
        });
        assertEqualsCode(SqlServiceCode.SYMBOL_ANALYZE_EXCEPTION, e);
    }

    private static void assertExplain(TgStatementMetadata actual) throws Exception {
        assertThrowsExactly(PlanGraphException.class, () -> {
            actual.getLowPlanGraph();
        });

        var list = actual.getColumnList();
        assertEquals(0, list.size());
    }
}
