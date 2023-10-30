package com.tsurugidb.iceaxe.test.scenario;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.TestInfo;
import static org.junit.jupiter.api.Assertions.fail;

import io.cucumber.java.After;
import io.cucumber.java.AfterAll;
import io.cucumber.java.Before;
import io.cucumber.java.BeforeAll;
import io.cucumber.java.Scenario;
import io.cucumber.java.en.Given;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;

public class StepDefs extends DbTestTableTester {
    private class TxState {
        public final TsurugiTransaction tx;
        public Future<Exception> commitDone = null;
        public TxState(TsurugiTransaction tx) { this.tx = tx; }
    }
    Map<String, TxState> txs = new HashMap<>();
    private TsurugiTransaction getTx(String txName) {
        var txst = txs.get(txName);
        if (txst == null) {
            throw new IllegalStateException("transaction " + txName + " is not defined");
        }
        return txst.tx;
    }
    private void setTxCommitDone(String txName, Future<Exception> commitDone) {
        var txst = txs.get(txName);
        if (txst == null) {
            throw new IllegalStateException("transaction " + txName + " is not defined");
        }
        txst.commitDone = commitDone;
    }

    String defaultTbl = null;

    @Before(order = 100)
    public void setUp(Scenario scenario) throws Exception {
        System.err.println("before name:" + scenario.getName());

        TestInfo info = new TestInfo(){
            @Override public Set<String> getTags() {
                return new HashSet<>(scenario.getSourceTagNames());
            }
            @Override public Optional<java.lang.reflect.Method> getTestMethod() {
                return Optional.empty();
            }
            @Override public Optional<Class<?>> getTestClass() {
                return Optional.empty();
            }
            @Override public String getDisplayName() {
                return scenario.getName();
            }
        };
        logInitStart(info);
        dropTestTable();
        createTable();
        logInitEnd(info);
    }
    private static void createTable() throws IOException, InterruptedException {
        executeDdl(getSession(), "CREATE TABLE " + TEST + "(pk CHAR(1) PRIMARY KEY, val VARCHAR(2))", TEST);
    }
    @After
    public void after() throws Exception {
        for (var e : txs.entrySet()) {
            var txst = e.getValue();
            System.err.println(e.getKey() + " discard");
            if (txst.commitDone != null) {
                txst.commitDone.get();
            } else {
                txst.tx.rollback();
                txst.tx.close();
            }
        }
        txs.clear();
    }

    @BeforeAll
    public static void beforeAll() {
        System.err.println("beforeAll");
    }

    @AfterAll
    public static void afterAll() {
        System.err.println("afterAll");
    }

    public void startTransaction(String txName, TgTxOption txOpt) throws Exception {
        System.err.println("BEGIN " + txOpt + " AS " + txName);
        var tx = getSession().createTransaction(txOpt);
        txs.put(txName, new TxState(tx));
    }

    public void setDefaultTable(String tblName) {
        System.err.println("set default table " + tblName);
        defaultTbl = tblName;
    }

    // SQL
    @Given("{word}: BEGIN/begin/START/start LTX/ltx/LONG/long")
    public void start_long_transaction(String txName) throws Exception {
        startTransaction(txName, TgTxOption.ofLTX(TEST).label(txName));
    }
    @Given("{word}: COMMIT/commit")
    public void commit_ok0(String txName) throws Exception {
        commit_ok(txName);
    }
    @Given("{word}: COMMIT/commit WILL/will OK/ok/SUCCESS/success")
    public void commit_ok(String txName) throws Exception {
        var tx = getTx(txName);
        System.err.println(txName + " COMMIT OK");
        txs.remove(txName);
        try {
            tx.commit(TgCommitType.DEFAULT);
        } catch (TsurugiTransactionException ex) {
            fail(txName + " COMMIT FAILED", ex);
            return;
        } finally {
            tx.close();
        }
    }
    @Given("{word}: COMMIT/commit WILL/will FAIL/fail")
    public void commit_fail(String txName) throws Exception {
        var tx = getTx(txName);
        System.err.println(txName + " COMMIT FAIL");
        txs.remove(txName);
        try {
            tx.commit(TgCommitType.DEFAULT);
        } catch (TsurugiTransactionException ex) {
            return;
        } finally {
            tx.close();
        }
        fail(txName + " COMMIT SUCCESSED");
    }
    @Given("{word}: COMMIT/commit WILL/will WAITING/waiting")
    public void commit_waiting(String txName) {
        System.err.println(txName + " COMMIT WAITING");
        var tx = getTx(txName);
        var future = executeFuture(() -> {
            try {
                tx.commit(TgCommitType.DEFAULT);
            } catch (Exception ex) {
                return ex;
            }
            return null;
        });
        setTxCommitDone(txName, future);
    }

    @Given("{word}: commit-wait returns ok")
    public void commit_wait_returns_ok(String txName) {
        System.err.println(txName + " COMMIT-WAIT OK");
        var txst = txs.get(txName);
        txs.remove(txName);
        if (!txst.commitDone.isDone()) {
            fail();
        }
        try {
            Exception res = txst.commitDone.get();
            if (res != null)
                fail("COMMIT-WAIT FAIL", res);
        } catch (Exception ex) {
            fail("interrupted", ex);
        }
    }

    @Given("{word}: commit-wait returns fail")
    public void commit_wait_returns_fail(String txName) {
        System.err.println(txName + " COMMIT-WAIT FAIL");
        var txst = txs.get(txName);
        txs.remove(txName);
        if (!txst.commitDone.isDone()) {
            try {
                Thread.sleep(40);
            } catch (InterruptedException ex) {}
            if (!txst.commitDone.isDone())
                fail();
        }
        try {
            Exception res = txst.commitDone.get();
            if (res == null)
                fail("COMMIT-WAIT OK");
        } catch (Exception ex) {
            fail("interrupted", ex);
        }
    }

    // this issue
    @Given("{word}: read full")
    public void read_full(String txName) throws Exception {
        System.err.println(txName + " READ FULL");
        var tx = getTx(txName);
        var sql = getSession().createQuery("SELECT COUNT(*) FROM " + TEST);
        tx.executeQuery(sql);
    }
    @Given("^([A-Za-z0-9]+): read ([A-Z]?)-([A-Z]?)$")
    public void read_range(String txName, String l, String r) throws Exception {
        System.err.println(txName + " READ RANGE " + l + "-" + r);
        String pred;
        if ("".equals(l)) {
            if ("".equals(r)) {
                throw new IllegalArgumentException("use read full for full scan");
            }
            pred = "WHERE pk <= '" + r + "'";
        } else {
            if ("".equals(r)) {
                pred = " WHERE '" + l + "' <= pk";
            } else {
                pred = " WHERE '" + l + "' <= pk AND pk <= '" + r + "'";
            }
        }
        var tx = getTx(txName);
        var sql = getSession().createQuery("SELECT COUNT(*) FROM " + TEST + " " + pred);
        tx.executeQuery(sql);
    }
    @Given("^([A-Za-z0-9]+): read ([A-Z])$")
    public void read_point(String txName, String key) throws Exception {
        System.err.println(txName + " READ POINT " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createQuery(
            "SELECT COUNT(*) FROM " + TEST + " WHERE pk = '" + key + "'"
            );
        tx.executeQuery(sql);
    }
    @Given("{word}: (write )insert {word}")
    public void write_insert(String txName, String key) throws Exception {
        System.err.println(txName + " INSERT " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createStatement(
            "INSERT INTO " + TEST + "(pk, val) VALUES('" + key + "', '" + key + txName.substring(txName.length() - 1, txName.length()) + "')"
            );
        tx.executeStatement(sql);
    }
    @Given("{word}: (write )upsert {word}")
    public void write_upsert(String txName, String key) throws Exception {
        System.err.println(txName + " INSERT " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createStatement(
            "INSERT OR REPLACE INTO " + TEST + "(pk, val) VALUES('" + key + "', '" + key + txName.substring(txName.length() - 1, txName.length()) + "')"
            );
        tx.executeStatement(sql);
    }
    @Given("{word}: (write )update {word}")
    public void write_update(String txName, String key) throws Exception {
        System.err.println(txName + " INSERT " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createStatement(
            "UPDATE " + TEST + " SET val = " + key + txName.substring(txName.length() - 1, txName.length()) + "' WHERE pk = '" + key + "'"
            );
        tx.executeStatement(sql);
    }
    @Given("{word}: (write )delete {word}")
    public void write_delete(String txName, String key) throws Exception {
        System.err.println(txName + " DELETE " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createStatement(
            "DELETE FROM " + TEST + " WHERE pk = '" + key + "'"
            );
        tx.executeStatement(sql);
    }
    @Given("prepare THE table with data:{}")
    public void prepare_table(String data) throws Exception {
        System.err.println("PREPARE TABLE " + data);
        var tx = getSession().createTransaction(TgTxOption.ofOCC());
        var sql = getSession().createStatement("DELETE FROM " + TEST);
        tx.executeStatement(sql);
        for (int i = 0; i < data.length(); i++) {
            char c = data.charAt(i);
            if ('A' <= c && c <= 'Z') {
                sql = getSession().createStatement(
                    "INSERT OR REPLACE INTO " + TEST + "(pk, val) VALUES('" + c + "', '" + c + "0')"
                    );
                tx.executeStatement(sql);
            } else if (c == ' ') {
                // skip
            } else {
                System.err.println("invalid key:" + c);
            }
        }
        tx.commit(TgCommitType.DEFAULT);
    }

}
