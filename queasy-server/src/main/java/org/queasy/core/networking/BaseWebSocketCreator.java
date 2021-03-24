package org.queasy.core.networking;

import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.queasy.core.config.WebSocketConfiguration;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public abstract class BaseWebSocketCreator implements WebSocketCreator {
    protected final WebSocketConfiguration wsConfig;

    public BaseWebSocketCreator(WebSocketConfiguration wsConfig) {
        this.wsConfig = wsConfig;
    }

    protected final boolean checkOrigin(ServletUpgradeRequest servletUpgradeRequest, ServletUpgradeResponse servletUpgradeResponse) {
        if (wsConfig.getOrigin() != null && !servletUpgradeRequest.isOrigin(wsConfig.getOrigin())) {
            servletUpgradeResponse.setSuccess(false);
            servletUpgradeResponse.setStatusCode(403);
            servletUpgradeResponse.setStatusReason("Invalid Origin");
            servletUpgradeResponse.complete();
            return false;
        }
        return true;
    }
}
