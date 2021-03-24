package org.queasy.core.networking;

import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.queasy.core.config.WebSocketConfiguration;
import org.queasy.core.managed.ConsumerGroup;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ConsumerGroupWebSocketCreator extends BaseWebSocketCreator {

    private final ConsumerGroup consumerGroup;

    public ConsumerGroupWebSocketCreator(WebSocketConfiguration wsConfig, ConsumerGroup consumerGroup) {
        super(wsConfig);
        this.consumerGroup = consumerGroup;
    }

    @Override
    public WebSocketListener createWebSocket(final ServletUpgradeRequest servletUpgradeRequest,
                                             final ServletUpgradeResponse servletUpgradeResponse) {
        return checkOrigin(servletUpgradeRequest, servletUpgradeResponse) ? new ConsumerGroupConnection(consumerGroup)
                : null;
    }

}
