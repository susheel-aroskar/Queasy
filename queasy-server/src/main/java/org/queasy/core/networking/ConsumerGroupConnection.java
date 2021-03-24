package org.queasy.core.networking;

import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.queasy.core.managed.ConsumerGroup;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ConsumerGroupConnection extends WebSocketAdapter {

    private final ConsumerGroup consumerGroup;

    public ConsumerGroupConnection(ConsumerGroup consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
}
