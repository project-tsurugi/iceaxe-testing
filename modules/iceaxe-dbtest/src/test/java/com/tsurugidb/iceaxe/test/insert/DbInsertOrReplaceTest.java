package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.test.util.DbTestSessions;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.function.TsurugiTransactionAction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert or replace test
 */
class DbInsertOrReplaceTest extends DbTestTableTester {

    private static final int SIZE = 10;
    private static final int INSERT_SIZE = 20;
    private static final String UPSERT_SQL = INSERT_SQL.replace("insert", "insert or replace");
    private static final String UPDATE_SQL = "update " + TEST + " set bar = :bar, zzz = :zzz where foo = :foo";

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @ParameterizedTest
    @ValueSource(ints = { 2, 10 })
    void occ(int threadSize) throws Exception {
        test(TgTxOption.ofOCC(), threadSize);
    }

    @RepeatedTest(10)
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertOrReplaceTest-ltx.*")
    void ltx() throws Exception {
        test(TgTxOption.ofLTX(TEST), 10);
    }

    @RepeatedTest(60)
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertOrReplaceTest-ltx.*")
    void ltx_2thread() throws Exception {
        test(TgTxOption.ofLTX(TEST), 2);
    }

    @RepeatedTest(100)
    @DisabledIfEnvironmentVariable(named = "ICEAXE_DBTEST_DISABLE", matches = ".*DbInsertOrReplaceTest-ltx.*")
    void ltx_3thread() throws Exception {
        test(TgTxOption.ofLTX(TEST), 3);
    }

    private void test(TgTxOption txOption, int threadSize) throws Exception {
        test(txOption, new DbTestSessions(), threadSize);
    }

    private void test(TgTxOption txOption, DbTestSessions sessions, int threadSize) throws Exception {
        try (sessions) {
            var onlineList = new ArrayList<OnlineTask>(threadSize);
            for (int i = 0; i < threadSize; i++) {
                var task = new OnlineTask(sessions.createSession(), txOption, i);
                onlineList.add(task);
            }

            var service = Executors.newCachedThreadPool();
            var futureList = new ArrayList<Future<?>>(threadSize);
            onlineList.forEach(task -> futureList.add(service.submit(task)));

            var exceptionList = new ArrayList<Exception>();
            for (var future : futureList) {
                try {
                    future.get();
                } catch (Exception e) {
                    exceptionList.add(e);
                }
            }
            if (!exceptionList.isEmpty()) {
                if (exceptionList.size() == 1) {
                    throw exceptionList.get(0);
                }
                var e = new Exception(exceptionList.stream().map(Exception::getMessage).collect(Collectors.joining("\n")));
                exceptionList.stream().forEach(e::addSuppressed);
                throw e;
            }
        }

        var actualList = selectAllFromTest();
        try {
            assertEquals(SIZE + INSERT_SIZE, actualList.size());
            TestEntity first = null;
            int i = 0;
            for (TestEntity actual : actualList) {
                if (i < SIZE) {
                    var expected = createTestEntity(i);
                    assertEquals(expected, actual);
                } else {
                    if (first == null) {
                        first = actual;
                    }
                    try {
                        assertEquals(first.getBar(), actual.getBar());
                        assertEquals(createZzz(first.getBar(), actual.getFoo() - SIZE), actual.getZzz());
                    } catch (AssertionError e) {
                        LOG.error("i={}, first={}, actual={}", i, first, actual, e);
                        throw e;
                    }
                }
                i++;
            }
        } catch (AssertionError e) {
            int i = 0;
            for (var actual : actualList) {
                LOG.error("actual[{}]={}", i, actual);
                i++;
            }
            throw e;
        }
    }

    private static class OnlineTask implements Callable<Void> {
        private final TsurugiSession session;
        private final TgTxOption txOption;
        private final int number;

        public OnlineTask(TsurugiSession session, TgTxOption txOption, int number) {
            this.session = session;
            this.txOption = txOption;
            this.number = number;
        }

        @Override
        public Void call() throws Exception {
            try (var insertPs = session.createStatement(UPSERT_SQL, INSERT_MAPPING)) {
                var setting = TgTmSetting.of(txOption);
                var tm = session.createTransactionManager(setting);

                tm.execute((TsurugiTransactionAction) transaction -> {
                    for (int i = 0; i < INSERT_SIZE; i++) {
                        var entity = new TestEntity(SIZE + i, number, createZzz(number, i));
                        int count = transaction.executeAndGetCount(insertPs, entity);
                        assertUpdateCount(1, count);
                    }
                });
            }
            return null;
        }
    }

    private static String createZzz(long number, int i) {
        return String.format("%d-%02d", number, i);
    }

    @Test
    void occ2() throws Exception {
        test2(TgTxOption.ofOCC(), true, 2);
    }

    @Test
    void occ2_commitReverse() throws Exception {
        test2(TgTxOption.ofOCC(), false, 1);
    }

    @Test
    void ltx2() throws Exception {
        test2(TgTxOption.ofLTX(TEST), true, 2);
    }

    @Test
    void ltx2_commitReverse() throws Exception {
        test2(TgTxOption.ofLTX(TEST), false, 2);
    }

    private void test2(TgTxOption txOption, boolean commitAsc, long expectedLastBar) throws Exception {
        var session = getSession();
        try (var insertPs = session.createStatement(UPSERT_SQL, INSERT_MAPPING); //
                var tx1 = session.createTransaction(txOption)) {
            tx1.getLowTransaction();
            try (var tx2 = session.createTransaction(txOption)) {
                tx2.getLowTransaction();

                int N1 = 1;
                int N2 = 2;

                int i = 0;
                var entity11 = new TestEntity(SIZE + i, N1, createZzz(N1, i));
                tx1.executeAndGetCount(insertPs, entity11);
                var entity21 = new TestEntity(SIZE + i, N2, createZzz(N2, i));
                tx2.executeAndGetCount(insertPs, entity21);

                i++;
                var entity22 = new TestEntity(SIZE + i, N2, createZzz(N2, i));
                tx2.executeAndGetCount(insertPs, entity22);
                var entity12 = new TestEntity(SIZE + i, N1, createZzz(N1, i));
                tx1.executeAndGetCount(insertPs, entity12);

                if (commitAsc) {
                    tx1.commit(TgCommitType.DEFAULT);
                    TimeUnit.MILLISECONDS.sleep(40);
                    assert2(1, true, SIZE);
                    tx2.commit(TgCommitType.DEFAULT);
                } else {
                    tx2.commit(TgCommitType.DEFAULT);
                    TimeUnit.MILLISECONDS.sleep(40);
                    assert2(2, true, SIZE);
                    tx1.commit(TgCommitType.DEFAULT);
                }
            }
        }

        assert2(expectedLastBar, false, SIZE + 2);
    }

    private void assert2(long expectedBar, boolean rtx, int maybeSize) throws IOException, InterruptedException {
        var actualList = selectAllFromTest(TgTmSetting.of(rtx ? TgTxOption.ofRTX() : TgTxOption.ofLTX()));
        try {
            assertEquals(SIZE + 2, actualList.size());
        } catch (AssertionError e) {
            if (rtx && actualList.size() == maybeSize) {
                // success
            } else {
                throw e;
            }
        }
        try {
            int i = 0;
            for (TestEntity actual : actualList) {
                if (i < SIZE) {
                    var expected = createTestEntity(i);
                    assertEquals(expected, actual);
                } else {
                    assertEquals(expectedBar, actual.getBar(), "bar[" + i + "]");
                }
                i++;
            }
        } catch (AssertionError e) {
            int i = 0;
            for (var actual : actualList) {
                LOG.error("actual[{}]={}", i, actual);
                i++;
            }
            throw e;
        }
    }

    @Test
    void updateOcc() throws Exception {
        updateOcc(true);
    }

    @Test
    void updateOcc_commitReverse() throws Exception {
        updateOcc(false);
    }

    private void updateOcc(boolean commitAsc) throws Exception {
        var session = getSession();

        try (var insertPs = session.createStatement(UPSERT_SQL, INSERT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL, INSERT_MAPPING); //
                var tx1 = session.createTransaction(TgTxOption.ofOCC())) {
            tx1.getLowTransaction();
            try (var tx2 = session.createTransaction(TgTxOption.ofOCC())) {
                tx2.getLowTransaction();

                int N1 = 1;
                int N2 = 2;

                var entity1 = new TestEntity(SIZE - 1, N1, createZzz(N1, 0));
                tx1.executeAndGetCount(updatePs, entity1);
                var entity2 = new TestEntity(SIZE - 1, N2, createZzz(N2, 0));
                tx2.executeAndGetCount(insertPs, entity2);

                if (commitAsc) {
                    tx1.commit(TgCommitType.DEFAULT);
                    TimeUnit.MILLISECONDS.sleep(40);
                    assertUpdate(1, true);
                    tx2.commit(TgCommitType.DEFAULT);
                } else {
                    tx2.commit(TgCommitType.DEFAULT);
                    TimeUnit.MILLISECONDS.sleep(40);
                    assertUpdate(2, true);
                    var e = assertThrows(TsurugiTransactionException.class, () -> {
                        tx1.commit(TgCommitType.DEFAULT);
                    });
                    assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e);
                }
            }
        }

        assertUpdate(2, false);
    }

    @Test
    void updateLtx() throws Exception {
        updateLtx(true);
    }

    @Test
    void updateLtx_commitReverse() throws Exception {
        updateLtx(false);
    }

    private void updateLtx(boolean commitAsc) throws Exception {
        var session = getSession();

        try (var insertPs = session.createStatement(UPSERT_SQL, INSERT_MAPPING); //
                var updatePs = session.createStatement(UPDATE_SQL, INSERT_MAPPING); //
                var tx1 = session.createTransaction(TgTxOption.ofLTX(TEST))) {
            tx1.getLowTransaction();
            try (var tx2 = session.createTransaction(TgTxOption.ofLTX(TEST))) {
                tx2.getLowTransaction();

                int N1 = 1;
                int N2 = 2;

                var entity1 = new TestEntity(SIZE - 1, N1, createZzz(N1, 0));
                tx1.executeAndGetCount(updatePs, entity1);
                var entity2 = new TestEntity(SIZE - 1, N2, createZzz(N2, 0));
                tx2.executeAndGetCount(insertPs, entity2);

                if (commitAsc) {
                    tx1.commit(TgCommitType.DEFAULT);
                    TimeUnit.MILLISECONDS.sleep(40);
                    assertUpdate(1, true);
                    tx2.commit(TgCommitType.DEFAULT);
                } else {
                    tx2.commit(TgCommitType.DEFAULT);
//                  TimeUnit.MILLISECONDS.sleep(40);
//                  assertUpdate(2, true);
                    tx1.commit(TgCommitType.DEFAULT);
                }
            }
        }

        assertUpdate(2, false);
    }

    private void assertUpdate(long expectedBar, boolean rtx) throws IOException, InterruptedException {
        var actualList = selectAllFromTest(TgTmSetting.of(rtx ? TgTxOption.ofRTX() : TgTxOption.ofLTX()));
        assertEquals(SIZE, actualList.size());
        try {
            int i = 0;
            for (TestEntity actual : actualList) {
                if (i < SIZE - 1) {
                    var expected = createTestEntity(i);
                    assertEquals(expected, actual);
                } else {
                    assertEquals(expectedBar, actual.getBar(), "bar[" + i + "]");
                }
                i++;
            }
        } catch (AssertionError e) {
            int i = 0;
            for (var actual : actualList) {
                LOG.error("actual[{}]={}", i, actual);
                i++;
            }
            throw e;
        }
    }
}
