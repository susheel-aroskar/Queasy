package org.queasy.core.network;

import com.google.common.annotations.VisibleForTesting;
import org.queasy.core.managed.QueueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ProducerConnection extends BaseWebSocketConnection {

    private final QueueWriter queueWriter;
    private final String qName;
    private final AtomicBoolean writingMessage;

    private static final Logger logger = LoggerFactory.getLogger(ProducerConnection.class);

    public ProducerConnection(QueueWriter queueWriter, String qName) {
        this.queueWriter = queueWriter;
        this.qName = qName;
        this.writingMessage = new AtomicBoolean();
    }

    @VisibleForTesting
    public String getQName() {
        return qName;
    }

    @Override
    public void onWebSocketText(final String message) {
        if (writingMessage.compareAndSet(false, true)) {
            try {
                if (queueWriter.publish(new String[]{qName, message})) {
                    sendStatus(Status.OK);
                } else {
                    sendStatus(Status.TIMEOUT);
                    logger.warn("Timed out while trying to write to the queue");
                }
            }
            catch (Exception ex) {
                logger.warn("Error while writing message to the queue");
                sendStatus(Status.ERROR);
            }
            finally {
                writingMessage.set(false);
            }
        } else {
            logger.warn("Received message from client before previous message was written.");
            sendStatus(Status.BUSY);
        }
    }

}
