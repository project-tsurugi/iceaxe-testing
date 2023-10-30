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
        public Future<?> commitDone = null;
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
    private void setTxCommitDone(String txName, Future<?> commitDone) {
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
        System.err.println("after");
        for (var e : txs.entrySet()) {
            var txst = e.getValue();
            System.err.println(e.getKey() + " discard");
            if (txst.commitDone != null) {
                txst.commitDone.get();
            } else {
                txst.tx.rollback();
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
        };
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
        };
        fail(txName + " COMMIT SUCCESSED");
    }
    @Given("{word}: COMMIT/commit WILL WAITING")
    public void commit_waiting(String txName) {
        System.err.println(txName + " COMMIT WAITING");
        var tx = getTx(txName);
        var future = executeFuture(() -> {
            tx.commit(TgCommitType.DEFAULT);
            return null;
        });
        setTxCommitDone(txName, future);
        
    }

    // this issue
    @Given("{word}: read full")
    public void read_full(String txName) throws Exception {
        System.err.println(txName + " READ FULL");
        var tx = getTx(txName);
        var sql = getSession().createQuery("SELECT COUNT(*) FROM " + TEST);
        tx.executeQuery(sql);
    }
    @Given("{word}: read {word}-{word}")
    public void read_range(String txName, String l, String r) throws Exception {
        System.err.println(txName + " READ RANGE " + l + "-" + r);
        if (!l.matches("^[A-Z]$")) throw new IllegalArgumentException("left:" + l + " is invalid");
        if (!r.matches("^[A-Z]$")) throw new IllegalArgumentException("right:" + r + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createQuery(
            "SELECT COUNT(*) FROM " + TEST + " WHERE '" + l + "' <= pk AND pk <= '" + l + "'"
            );
        tx.executeQuery(sql);
    }
    @Given("{word}: write insert {word}")
    public void write_insert(String txName, String key) throws Exception {
        System.err.println(txName + " INSERT " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createStatement(
            "INSERT INTO " + TEST + "(pk, val) VALUES('" + key + "', '" + key + txName.substring(txName.length() - 1, txName.length()) + "')"
            );
        tx.executeStatement(sql);
    }
    @Given("{word}: write upsert {word}")
    public void write_upsert(String txName, String key) throws Exception {
        System.err.println(txName + " INSERT " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createStatement(
            "INSERT OR REPLACE INTO " + TEST + "(pk, val) VALUES('" + key + "', '" + key + txName.substring(txName.length() - 1, txName.length()) + "')"
            );
        tx.executeStatement(sql);
    }
    @Given("{word}: write delete {word}")
    public void write_delete(String txName, String key) throws Exception {
        System.err.println(txName + " DELETE " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        var sql = getSession().createStatement(
            "DELETE FROM " + TEST + " WHERE pk = '" + key + "'"
            );
        tx.executeStatement(sql);
    }
    @Given("prepare THE table with data: {}")
    public void prepare_table(String data) throws Exception {
        System.err.println("PREPARE TABLE " + data);
        var tx = getSession().createTransaction(TgTxOption.ofOCC());
        for (int i = 0; i < data.length(); i++) {
            char c = data.charAt(i);
            if ('A' <= c && c <= 'Z') {
                var sql = getSession().createStatement(
                    "INSERT OR REPLACE INTO " + TEST + "(pk, val) VALUES('" + c + "', '" + c + "0')"
                    );
                tx.executeStatement(sql);
            } else {
                System.err.println("invalid key:" + c);
            }
        }
        tx.commit(TgCommitType.DEFAULT);
    }

}
