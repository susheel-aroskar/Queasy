package org.queasy.client;

import org.queasy.core.network.Command;
import org.queasy.core.network.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Blocking Consumer group client
 *
 * @author saroskar
 * Created on: 2021-04-06
 */
public final class ConsumerGroupMember extends BaseQueueClient {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroupMember.class);


    /**
     * Blocks till this client receives a message from the server
     *
     * @param timeout in milliseconds
     * @return
     * @throws TimeoutException
     * @throws IOException
     */
    synchronized public String readMessage(final long timeout) throws TimeoutException, IOException {
        try {
            if (message == null) {
                getQueueConnection().getRemote().sendString(Command.DEQUEUE.toString());
                wait(timeout);
            }

            if (message == null) {
                throw new TimeoutException("Client side timeout");
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
            return message;
        } catch (InterruptedException ex) {
            logger.info("Client side interruption");
            throw new TimeoutException("Client side interruption");
        } finally {
            message = null;
        }
    }

    public String readMessage() throws TimeoutException, IOException {
        return readMessage(Integer.MAX_VALUE);
    }

}
