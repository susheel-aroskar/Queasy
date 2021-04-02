package org.queasy.core.network;

import org.queasy.core.managed.ConsumerGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ConsumerConnection extends BaseWebSocketConnection {

    private final ConsumerGroup consumerGroup;
    private final AtomicBoolean awaitingMessage;

    public static final String GET_COMMAND = Command.GET.toString();
    private static final Logger logger = LoggerFactory.getLogger(ProducerConnection.class);


    public ConsumerConnection(ConsumerGroup consumerGroup) {
        this.consumerGroup = consumerGroup;
        this.awaitingMessage = new AtomicBoolean();
    }

    @Override
    public void onWebSocketText(final String message) {
        if (GET_COMMAND.equals(message) && awaitingMessage.compareAndSet(false, true)) {
            try {
                consumerGroup.waitForMessage(this);
            }
            catch (Exception ex) {
                awaitingMessage.set(false);
                sendStatus(Status.ERROR);
                logger.warn("Error while waiting for a message");
            }
        }
    }

    public void sendMessage(final String message) {
        awaitingMessage.set(false);
        writeMessage(message);
    }

}
