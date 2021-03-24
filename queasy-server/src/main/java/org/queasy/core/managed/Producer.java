package org.queasy.core.managed;

import io.dropwizard.lifecycle.Managed;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.queasy.core.config.QueueConfiguration;
import org.queasy.core.mesg.BinaryMessage;
import org.queasy.core.mesg.Message;
import org.queasy.core.mesg.TextMessage;
import org.queasy.db.QueueSchema;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class Producer implements Managed, Runnable {

    private final QueueConfiguration queueConfiguration;
    private final QueueSchema queueSchema;
    private final ArrayBlockingQueue<Message> queue;
    private final Jdbi jdbi;

    private volatile boolean shutdownFlag;

    /**
     * milliseconds since epoch * 100,000
     */
    private volatile long currentMessageId;


    public Producer(QueueConfiguration queueConfiguration, QueueSchema queueSchema) {
        this.queueConfiguration = queueConfiguration;
        this.queueSchema = queueSchema;
        queue = new ArrayBlockingQueue<>(queueConfiguration.getRingBufferSize());
        jdbi = queueSchema.getJdbi();
    }

    public long getCurrentMessageId() {
        return currentMessageId;
    }

    private static long nextMessageIdTimePart() {
        return System.currentTimeMillis() * 100000;
    }

    @Override
    public void start() throws Exception {
        shutdownFlag = false;
        currentMessageId = nextMessageIdTimePart();
    }


    @Override
    public void stop() throws Exception {
        shutdownFlag = true;
        //TODO: interrupt threads
    }

    public void enqueue(final BinaryMessage message) throws InterruptedException {
        queue.put(message);
    }

    public void enqueue(final TextMessage message) throws InterruptedException {
        queue.put(message);
    }

    public void run() {
        final Handle handle = jdbi.open();

        while (!shutdownFlag) {
            try {
                Message message = queue.take();

                long messageId = nextMessageIdTimePart();
                messageId = (messageId > currentMessageId) ? messageId : currentMessageId;

                int i = 0;
                handle.begin();
                final PreparedBatch batch = handle.prepareBatch("");
                while (message != null && i < queueConfiguration.getRingBufferSize()) { //TODO: batchSize
                    i++;
                    messageId++;
                    //TODO: add messages to transaction batch
                    addMessageToBatch(message, batch);
                    message = queue.poll();
                }

                batch.execute();
                handle.commit();
                currentMessageId = messageId;
            }
            catch (InterruptedException ex) {
                //check for shutdown flag at the top of the loop
            }
        }
    }

    private void addMessageToBatch(Message message, PreparedBatch batch) {

    }

//    private void addMessageToBatch(BinaryMessage message, PreparedBatch batch) {
//
//    }

}
