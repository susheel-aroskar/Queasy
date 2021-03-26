package org.queasy.core.network;

import com.google.common.annotations.VisibleForTesting;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
public abstract class BaseWebSocketConnection extends WebSocketAdapter implements WriteCallback {

    private final static AtomicInteger connectionCount = new AtomicInteger(0);
    private static final Logger logger = LoggerFactory.getLogger(BaseWebSocketConnection.class);

    private final AtomicBoolean countIncremented;

    protected BaseWebSocketConnection() {
        countIncremented = new AtomicBoolean();
    }

    public static int getConnectionCount() {
        return connectionCount.get();
    }

    @VisibleForTesting
    public static void resetConnectionCount() {
        connectionCount.set(0);
    }


    public final void writeMessage(final String message) {
        getRemote().sendString(message, this);
    }

    @Override
    public void writeFailed(final Throwable t) {
        logger.error("Error while writing to client", t);
        getSession().close();
    }

    @Override
    public void writeSuccess() {
        //override in subclass, if necessary
    }

    protected final void sendStatus(final Status status) {
        writeMessage(status.toString());
    }

    @Override
    public final void onWebSocketConnect(Session sess) {
        if (countIncremented.compareAndSet(false, true)) {
            final int count = connectionCount.incrementAndGet();
            logger.debug("WEBSOCKET CONNECT #{}", count);
        }
        super.onWebSocketConnect(sess);
    }

    @Override
    public final void onWebSocketClose(int statusCode, String reason) {
        if (countIncremented.compareAndSet(true, false)) {
            final int count = connectionCount.decrementAndGet();
            logger.debug("WEBSOCKET CLOSE #{}", count);
        }
        super.onWebSocketClose(statusCode, reason);
    }

    @Override
    public final void onWebSocketError(Throwable cause) {
        if (countIncremented.compareAndSet(true, false)) {
            final int count = connectionCount.decrementAndGet();
            logger.debug("WEBSOCKET ERROR #{}", count);
        }
        super.onWebSocketError(cause);
    }

}
