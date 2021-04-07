package org.queasy.client;

import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.queasy.core.network.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author saroskar
 * Created on: 2021-04-06
 */
public final class TopicSubscription extends WebSocketAdapter {

    private final TopicSubscriber topicSubscriber;

    private static final Logger logger = LoggerFactory.getLogger(TopicSubscription.class);


    public TopicSubscription(final TopicSubscriber topicSubscriber) {
        this.topicSubscriber = topicSubscriber;
    }

    @Override
    public void onWebSocketError(final Throwable cause) {
        topicSubscriber.onError(cause);
    }

    @Override
    public void onWebSocketText(final String message) {
        if (Status.MESG_DROP.toString().equals(message)) {
            topicSubscriber.onDroppedMessages();
        } else if (Status.ERROR.toString().equals(message)) {
            topicSubscriber.onError(new IOException("Server I/O error"));
        } else {
            topicSubscriber.onMessage(message);
        }
    }

    @Override
    public void onWebSocketClose(final int statusCode, final String reason) {
        topicSubscriber.onClose();
        logger.info("Websocket connection closed. status: {}, reason: {}", statusCode, reason);
    }

}
