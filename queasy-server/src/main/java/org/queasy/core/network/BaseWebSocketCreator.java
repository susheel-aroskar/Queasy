package org.queasy.core.network;

import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public abstract class BaseWebSocketCreator implements WebSocketCreator {

    protected final String origin;
    private final int maxConnections;

    public BaseWebSocketCreator(final String origin, final int maxConnections) {
        this.origin = origin;
        this.maxConnections = maxConnections;
    }

    protected final boolean checkOrigin(final ServletUpgradeRequest req, final ServletUpgradeResponse resp) {
        if (origin == null || !origin.equals(req.getOrigin())) {
            closeConnection(403, resp);
            return false;
        }
        return true;
    }

    protected final boolean checkMaxConnectionsLimit(final ServletUpgradeResponse resp) {
        if (BaseWebSocketConnection.getConnectionCount() >= maxConnections) {
            closeConnection(503, resp);
            return false;
        }
        return true;
    }

    protected void closeConnection(final int status, final ServletUpgradeResponse resp) {
        resp.setSuccess(false);
        resp.setStatusCode(status);
        resp.complete();
    }

    @Override
    public final WebSocketListener createWebSocket(final ServletUpgradeRequest req, final ServletUpgradeResponse resp) {
        return (checkOrigin(req, resp) && checkMaxConnectionsLimit(resp)) ? createConnection(req, resp) : null;
    }

    protected abstract WebSocketListener createConnection(final ServletUpgradeRequest req, final ServletUpgradeResponse resp);

}
