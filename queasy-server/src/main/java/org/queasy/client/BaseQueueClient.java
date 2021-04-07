package org.queasy.client;

import java.util.function.Consumer;

/**
 * @author saroskar
 * Created on: 2021-04-07
 */
abstract class BaseQueueClient implements Consumer<String> {

    private final QueueConnection qConn;
    protected volatile String message;

    BaseQueueClient() {
        this.qConn = new QueueConnection(this);
    }

    final QueueConnection getQueueConnection() {
        return qConn;
    }

    final public boolean isConnected() {
        return qConn.isConnected();
    }

    @Override
    synchronized final public void accept(final String message) {
        this.message = message;
        notifyAll();
    }

}
