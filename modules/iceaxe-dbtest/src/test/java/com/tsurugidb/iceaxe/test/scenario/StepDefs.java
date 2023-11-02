package com.tsurugidb.iceaxe.test.scenario;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.TestInfo;
import static org.junit.jupiter.api.Assertions.fail;

import io.cucumber.java.After;
import io.cucumber.java.AfterAll;
import io.cucumber.java.Before;
import io.cucumber.java.BeforeAll;
import io.cucumber.java.Scenario;
import io.cucumber.java.en.Given;
import io.cucumber.java.ParameterType;

import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
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

    @ParameterType("[A-Za-z0-9]+") public String txName(String str) { return str; }
    @ParameterType("[A-Z]") public String key(String str) { return str; }
    @ParameterType("[A-Z ]*")
    public List<String> dataset(String str) {
        return Stream.of(str.split("")).filter( s -> !" ".equals(s)).collect(Collectors.toList());
    }
    // full scan:          "full"
    // simple close range: "A-Z", "A-", "-Z"
    // full range:         "(A:Z)", "[A-inf]"
    @ParameterType("full|[A-Z]?-[A-Z]?|[\\[(](?:[A-Z]|-?inf)[-,:](?:[A-Z]|\\+?inf)[\\])]")
    public ScanRange range(String str) {
        if ("full".equals(str)) return new ScanRange(null, null);
        if (str.matches("[A-Z]-[A-Z]")) {
            return new ScanRange(new ScanEndPoint(false, str.substring(0, 1)), new ScanEndPoint(false, str.substring(2, 3)));
        } else if (str.matches("[A-Z]-")) {
            return new ScanRange(new ScanEndPoint(false, str.substring(0, 1)), null);
        } else if (str.matches("-[A-Z]")) {
            return new ScanRange(null, new ScanEndPoint(false, str.substring(1, 2)));
        } else if (str.matches("[\\[(][A-Z][-,:][A-Z][\\])]")) {
            return new ScanRange(new ScanEndPoint(str.charAt(0) == '(', str.substring(1, 2)),
                                 new ScanEndPoint(str.charAt(4) == ')', str.substring(3, 4)));
        } else if (str.matches("[\\[(]-?inf[-,:][A-Z][\\])]")) {
            int n = str.length();
            return new ScanRange(null, new ScanEndPoint(str.charAt(n-1) == ')', str.substring(n-2, n-1)));
        } else if (str.matches("[\\[(][A-Z][-,:]\\+?inf[\\])]")) {
            return new ScanRange(new ScanEndPoint(str.charAt(0) == '(', str.substring(1, 2)), null);
        }
        throw new IllegalStateException("never reached");
    }

    public static class ScanRange {
        final ScanEndPoint l;
        final ScanEndPoint r;
        ScanRange(ScanEndPoint l, ScanEndPoint r) { this.l = l; this.r = r; }
        @Override public String toString() {
            return (l == null ? "[-inf" : ((l.open ? "(" : "[") + l.point)) + ":"
                 + (r == null ? "+inf]" : (r.point + (r.open ? ")" : "]")));
        }
        String pred(String colName) {
            String predL = l == null ? null : ("'" + l.point + "' " + (l.open ? "<" : "<=") + " " + colName);
            String predR = r == null ? null : (colName + " " + (r.open ? "<" : "<=") + " '" + r.point + "'");
            if (predL == null && predR == null) {
                return "";
            } else if (predL == null) {
                return " WHERE " + predR;
            } else if (predR == null) {
                return " WHERE " + predL;
            } else {
                return " WHERE " + predL + " AND " + predR;
            }
        }
    }
    public static class ScanEndPoint {
        final boolean open;
        final String point;
        ScanEndPoint(boolean open, String point) { this.open = open; this.point = point;}
    }

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
    private TsurugiStatementResult executeStatement(TsurugiTransaction tx, String sql) throws IOException, InterruptedException, TsurugiTransactionException {
        try (var statement = getSession().createStatement(sql)) {
            return tx.executeStatement(statement);
        }
    }
    private TsurugiQueryResult<?> executeQuery(TsurugiTransaction tx, String sql) throws IOException, InterruptedException, TsurugiTransactionException {
        try (var query = getSession().createQuery(sql)) {
            return tx.executeQuery(query);
        }
    }
    @After
    public void after() throws Exception {
        for (var e : txs.entrySet()) {
            var txst = e.getValue();
            System.err.println(e.getKey() + " discard");
            if (txst.commitDone != null) {
                txst.commitDone.get();
                txst.tx.close();
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

    // public void setDefaultTable(String tblName) {
    //     System.err.println("set default table " + tblName);
    //     defaultTbl = tblName;
    // }

    // SQL
    @Given("{txName}: BEGIN/begin/START/start LTX/ltx/LONG/long")
    public void start_ltx(String txName) throws Exception {
        startTransaction(txName, TgTxOption.ofLTX(TEST).label(txName));
    }
    @Given("{txName}: BEGIN/begin/START/start LTX/ltx/LONG/long wp")
    public void start_ltx_wp(String txName) throws Exception {
        startTransaction(txName, TgTxOption.ofLTX(TEST).label(txName));
    }
    @Given("{txName}: BEGIN/begin/START/start")
    public void start_default_transaction(String txName) throws Exception {
        startTransaction(txName, TgTxOption.ofOCC().label(txName));
    }
    @Given("{txName}: BEGIN/begin/START/start OCC/occ/SHORT/short")
    public void start_short_transaction(String txName) throws Exception {
        startTransaction(txName, TgTxOption.ofOCC().label(txName));
    }

    @Given("{txName}: COMMIT/commit")
    public void commit_ok0(String txName) throws Exception {
        commit_ok(txName);
    }
    @Given("{txName}: commit will ok/success")
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
    @Given("{txName}: commit will fail")
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
    @Given("{txName}: commit will waiting")
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

    @Given("{txName}: commit-wait returns ok")
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

    @Given("{txName}: commit-wait returns fail")
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

    @Given("{txName}: abort/rollback")
    public void abort(String txName) throws Exception {
        System.err.println(txName + " ABORT");
        var tx = getTx(txName);
        txs.remove(txName);
        tx.rollback();
    }

    // this issue
    @Given("{txName}: read {range}")
    public void read_range(String txName, ScanRange range) throws Exception {
        System.err.println(txName + " READ RANGE " + range);
        var tx = getTx(txName);
        executeQuery(tx, "SELECT COUNT(*) FROM " + TEST + range.pred("pk"));
    }
    @Given("{txName}: read {key}")
    public void read_point(String txName, String key) throws Exception {
        System.err.println(txName + " READ POINT " + key);
        if (!key.matches("^[A-Z]$")) throw new IllegalArgumentException("key:" + key + " is invalid");
        var tx = getTx(txName);
        executeQuery(tx, "SELECT COUNT(*) FROM " + TEST + " WHERE pk = '" + key + "'");
    }
    @Given("{txName}: (write )insert {key}")
    public void write_insert(String txName, String key) throws Exception {
        System.err.println(txName + " INSERT " + key);
        var tx = getTx(txName);
        String newVal = key + txName.charAt(txName.length() - 1);
        executeStatement(tx, "INSERT INTO " + TEST + "(pk, val) VALUES('" + key + "', '" + newVal + "')");
    }
    @Given("{txName}: (write )upsert {key}")
    public void write_upsert(String txName, String key) throws Exception {
        System.err.println(txName + " UPSERT " + key);
        var tx = getTx(txName);
        String newVal = key + txName.charAt(txName.length() - 1);
        executeStatement(tx, "INSERT OR REPLACE INTO " + TEST + "(pk, val) VALUES('" + key + "', '" + newVal + "')");
    }
    @Given("{txName}: (write )update {key}")
    public void write_update(String txName, String key) throws Exception {
        System.err.println(txName + " UPDATE " + key);
        var tx = getTx(txName);
        String newVal = key + txName.charAt(txName.length() - 1);
        executeStatement(tx, "UPDATE " + TEST + " SET val = '" + newVal + "' WHERE pk = '" + key + "'");
    }
    @Given("{txName}: (write )delete {key}")
    public void write_delete(String txName, String key) throws Exception {
        System.err.println(txName + " DELETE " + key);
        var tx = getTx(txName);
        executeStatement(tx, "DELETE FROM " + TEST + " WHERE pk = '" + key + "'");
    }
    @Given("{txName}: (write )delete {range}")
    public void write_delete_range(String txName, ScanRange range) throws Exception {
        System.err.println(txName + " DELETE " + range);
        var tx = getTx(txName);
        executeStatement(tx, "DELETE FROM " + TEST + range.pred("pk"));
    }
    @Given("prepare THE empty table")
    public void prepare_the_empty_table() throws Exception {
        prepare_the_table(List.of());
    }
    @Given("prepare THE table with data:{dataset}")
    public void prepare_the_table_with_data(List<String> dataset) throws Exception {
        prepare_the_table(dataset);
    }
    public void prepare_the_table(List<String> dataset) throws Exception {
        System.err.println("PREPARE TABLE " + dataset);
        try (var tx = getSession().createTransaction(TgTxOption.ofOCC())) {
            executeStatement(tx, "DELETE FROM " + TEST);
            for (String c : dataset) {
                executeStatement(tx, "INSERT OR REPLACE INTO " + TEST + "(pk, val) VALUES('" + c + "', '" + c + "')");
            }
            tx.commit(TgCommitType.DEFAULT);
        }
    }

}
