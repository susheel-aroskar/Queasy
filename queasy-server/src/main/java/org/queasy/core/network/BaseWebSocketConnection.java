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

    private final AtomicBoolean isConnected;

    protected BaseWebSocketConnection() {
        isConnected = new AtomicBoolean();
    }

//    public boolean isClosed() {
//        return !isConnected.get();
//    }

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
    public void writeSuccess() {
        //override in subclass, if necessary
    }

    @Override
    public void writeFailed(final Throwable t) {
        getSession().close();
        logger.error("Error while writing to client", t);
    }


    protected final void sendStatus(final Status status) {
        writeMessage(status.toString());
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        super.onWebSocketConnect(sess);
        if (isConnected.compareAndSet(false, true)) {
            final int count = connectionCount.incrementAndGet();
            logger.debug("WEBSOCKET CONNECT #{}", count);
        }
    }

    @Override
    public final void onWebSocketClose(int statusCode, String reason) {
        super.onWebSocketClose(statusCode, reason);
        if (isConnected.compareAndSet(true, false)) {
            final int count = connectionCount.decrementAndGet();
            logger.debug("WEBSOCKET CLOSE #{}", count);
        }
    }

    @Override
    public final void onWebSocketError(Throwable cause) {
        super.onWebSocketError(cause);
        if (isConnected.compareAndSet(true, false)) {
            final int count = connectionCount.decrementAndGet();
            logger.debug("WEBSOCKET ERROR #{}", count);
        }
    }

}
