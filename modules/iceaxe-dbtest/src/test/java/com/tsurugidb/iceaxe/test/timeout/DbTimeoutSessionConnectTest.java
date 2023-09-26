package com.tsurugidb.iceaxe.test.timeout;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;

/**
 * session connect timeout test
 */
public class DbTimeoutSessionConnectTest extends DbTimetoutTest {

    @Test
    void timeoutDefault() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.SESSION_CONNECT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySession(TsurugiSession session) {
                session.setConnectTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected TsurugiConnector getTsurugiConnector(PipeServerThtread pipeServer) {
        pipeServer.setPipeWrite(false);
        return super.getTsurugiConnector(pipeServer);
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try {
            session.getLowSqlClient();
        } catch (IOException e) {
            assertInstanceOf(TimeoutException.class, e.getCause());
            LOG.trace("timeout success");
            assertFalse(session.isAlive());
            return;
        } finally {
            pipeServer.setPipeWrite(true);
        }
        fail("didn't time out");
    }
}
