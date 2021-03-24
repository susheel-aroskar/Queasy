package org.queasy.core.networking;

import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.queasy.core.config.WebSocketConfiguration;
import org.queasy.core.managed.Producer;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ProducerWebSocketCreator extends BaseWebSocketCreator {

    private final Producer producer;

    public ProducerWebSocketCreator(final WebSocketConfiguration wsConfig, final Producer producer) {
        super(wsConfig);
        this.producer = producer;
    }

    @Override
    public WebSocketListener createWebSocket(final ServletUpgradeRequest servletUpgradeRequest,
                                             final ServletUpgradeResponse servletUpgradeResponse) {
        return checkOrigin(servletUpgradeRequest, servletUpgradeResponse) ? new ProducerConnection(producer) : null;
    }
}
