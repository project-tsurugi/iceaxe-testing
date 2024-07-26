package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.opentest4j.AssertionFailedError;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * server stop (transaction) test
 */
public class DbServerStopTransactionTest extends DbTimetoutTest {

    private static final int EXPECTED_TIMEOUT = 1;

    // サーバーが停止した場合に即座にエラーが返ることを確認するテスト
    @Test
    @Timeout(value = EXPECTED_TIMEOUT, unit = TimeUnit.SECONDS)
    void serverStop() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.DEFAULT, EXPECTED_TIMEOUT + 1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        session.getLowSqlClient();

        pipeServer.setPipeWrite(false);
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            pipeServer.close(); // server stop

            boolean ioe = false;
            try {
                transaction.getLowTransaction();
            } catch (IOException e) {
                ioe = true;
                try {
                    assertEquals("lost connection", e.getMessage());
                } catch (AssertionFailedError t) {
                    t.addSuppressed(e);
                    throw t;
                }
                return;
            } finally {
                pipeServer.setPipeWrite(true);

                session.close();

                assertTrue(ioe);
            }

            fail("didn't I/O error");
        }
    }
}
