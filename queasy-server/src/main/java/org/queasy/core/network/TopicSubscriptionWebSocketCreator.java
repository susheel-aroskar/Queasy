package org.queasy.core.network;

import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.queasy.core.managed.Topic;

/**
 * @author saroskar
 * Created on: 2021-04-06
 */
public class TopicSubscriptionWebSocketCreator extends BaseWebSocketCreator {

    private final Topic topic;

    public TopicSubscriptionWebSocketCreator(String origin, int maxConnections, final Topic topic) {
        super(origin, maxConnections);
        this.topic = topic;
    }

    @Override
    protected WebSocketListener createConnection(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
        return new TopicSubscription(topic);
    }
}
