package org.queasy.client;

import org.queasy.core.network.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author saroskar
 * Created on: 2021-04-06
 */
public final class QueueProducer extends BaseQueueClient {

    private static final Logger logger = LoggerFactory.getLogger(QueueProducer.class);


    synchronized public void writeMessage(final String mesgToEnqueue, long timeout) throws IOException, TimeoutException, IllegalStateException {
        try {
            if (this.message == null) {
                getQueueConnection().getRemote().sendString(mesgToEnqueue);
                wait(timeout);
            }

            if (this.message == null) {
                throw new TimeoutException("Client side timeout. Server may still be busy writing this message " +
                        "and may not accept new messages till it is done!");
            }
            if (Status.OK.toString().equals(this.message)) {
                return; // write successful, OK to write new message
            }
            if (Status.BUSY.toString().equals(this.message)) {
                throw new IllegalStateException("Attempt to write a new message before ACK for the previous message was received.");
            }
            if (Status.TIMEOUT.toString().equals(message)) {
                throw new TimeoutException("Server side timeout");
            }
            if (Status.CLOSE.toString().equals(message)) {
                throw new EOFException("Server closed connection");
            }
            if (Status.ERROR.toString().equals(message)) {
                throw new IOException("Server I/O error");
            }
        } catch (InterruptedException ex) {
            logger.info("Client side interruption");
            throw new TimeoutException("Client side interruption");
        } finally {
            this.message = null;
        }
    }

    public void writeMessage(final String message) throws IOException, TimeoutException {
        writeMessage(message, Long.MAX_VALUE);
    }

}
