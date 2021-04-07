package org.queasy.client;

import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.queasy.core.network.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * @author saroskar
 * Created on: 2021-04-07
 */
final class QueueConnection extends WebSocketAdapter {

    private final Consumer<String> mesgConsumer;

    private static final Logger logger = LoggerFactory.getLogger(QueueConnection.class);


    QueueConnection(final Consumer<String> mesgConsumer) {
        this.mesgConsumer = mesgConsumer;
    }

    @Override
    public void onWebSocketError(final Throwable cause) {
        mesgConsumer.accept(Status.ERROR.toString());
        logger.error("Network communication error", cause);
    }

    @Override
    public void onWebSocketText(final String message) {
        mesgConsumer.accept(message);
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        mesgConsumer.accept(Status.CLOSE.toString());
        logger.info("Websocket connection closed. status: {}, reason: {}", statusCode, reason);
    }

}
