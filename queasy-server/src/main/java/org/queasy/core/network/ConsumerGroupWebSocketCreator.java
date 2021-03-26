package org.queasy.core.network;

import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.queasy.core.managed.ConsumerGroup;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ConsumerGroupWebSocketCreator extends BaseWebSocketCreator {

    private final ConsumerGroup consumerGroup;

    public ConsumerGroupWebSocketCreator(final String origin, final int maxConnections, final ConsumerGroup consumerGroup) {
        super(origin, maxConnections);
        this.consumerGroup = consumerGroup;
    }

    @Override
    protected WebSocketListener createConnection(final ServletUpgradeRequest req, final ServletUpgradeResponse resp) {
        return new ConsumerConnection(consumerGroup);
    }

}
