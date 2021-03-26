package org.queasy.core.managed;

import io.dropwizard.util.Strings;
import org.jdbi.v3.core.Jdbi;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.QueueConfiguration;
import org.queasy.core.network.ConsumerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public final class ConsumerGroup implements Runnable {

    private final String consumerGroupName;
    private final ConsumerGroupConfiguration cgConfig;
    private final Jdbi jdbi;
    private final String loadMessagesSQL;
    private final QueueWriter queueWriter;

    private volatile long lastReadMessageId;


    private final ArrayBlockingQueue<String> messages;
    private final LinkedBlockingQueue<ConsumerConnection> clients;

    private static final String SELECT_CHECKPOINT_SQL = "SELECT checkpoint FROM queasy_checkpoint WHERE cg_name = ?";
    private static final String INSERT_CHECKPOINT_SQL = "INSERT INTO queasy_checkpoint (cg_name, checkpoint, ts) " +
            "VALUES (?, ?, ?)";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);


    public ConsumerGroup(final String consumerGroupName, final ConsumerGroupConfiguration cgConfig,
                         final QueueConfiguration queueConfiguration, final Jdbi jdbi, final QueueWriter queueWriter) {
        this.consumerGroupName = consumerGroupName;
        this.cgConfig = cgConfig;
        this.jdbi = jdbi;
        this.queueWriter = queueWriter;
        this.messages = new ArrayBlockingQueue<>(cgConfig.getSelectBatchSize() + 1);
        this.clients = new LinkedBlockingQueue<>();
        this.loadMessagesSQL = "SELECT id, mesg FROM " + queueConfiguration.getTableName() +
                " WHERE id > ? AND qname = ? AND type is NULL" +
                (Strings.isNullOrEmpty(cgConfig.getQuery()) ? ""  : " AND " + cgConfig.getQuery()) +
                " ORDER BY id ASC";

        this.lastReadMessageId = lastCheckpoint(consumerGroupName, jdbi, queueWriter.getCurrentId());
    }

    private static long lastCheckpoint(final String consumerGroupName, final Jdbi jdbi, long writerMesgId) {
        final Optional<Long> result = jdbi.withHandle(handle -> handle.select(SELECT_CHECKPOINT_SQL, consumerGroupName)
                .map((rs, col, ctx) -> rs.getLong(col))
                .findOne());
        if (result.isPresent()) {
            return result.get();
        }

        //No checkpoint established. Use producer's currentId as default and checkpoint it to the DB
        jdbi.withHandle(handle ->
                handle.execute(INSERT_CHECKPOINT_SQL, consumerGroupName, writerMesgId, System.currentTimeMillis()));
        return writerMesgId;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public boolean waitForMessage(final ConsumerConnection client) throws InterruptedException {
        final String message = messages.poll();
        if (message != null) {
            client.sendMessage(message);
            return true;
        } else {
            //add client to wait queue
            clients.put(client);
            return false;
        }
    }

    private boolean loadNextBatchOfMessages() {
        if (queueWriter.getCurrentId() <= lastReadMessageId) {
            return false;
        }

        final long oldLastReadMessageId = lastReadMessageId;
        jdbi.useHandle(handle ->
                handle.select(loadMessagesSQL, lastReadMessageId, cgConfig.getQueueName())
                        .setFetchSize(cgConfig.getSelectBatchSize())
                        .setMaxRows(cgConfig.getSelectBatchSize())
                        .map((rs, ctx) -> {
                            lastReadMessageId = rs.getLong(1);
                            return rs.getString(2);
                        })
                        .forEach(s -> {
                            try {
                                messages.put(s);
                            } catch (InterruptedException ex) {
                                logger.warn("Interrupted exception while loading messages from DB", ex);
                            }
                        })
        );

        return (lastReadMessageId > oldLastReadMessageId);
    }

    public void run() {
        while (true) {
            try {
                final ConsumerConnection client = clients.poll();
                if (client == null) {
                    break; //there are no clients waiting for messages, bail out
                }

                final String message = messages.poll();
                if (message != null) {
                    client.sendMessage(message); // send the message to the client
                    continue; //next
                }

                // we have run out of fetched messages, add the client back to wait queue
                clients.put(client);

                //Try to load more messages from DB
                if (!loadNextBatchOfMessages()) {
                    //There are no more messages left, bail out
                    break;
                }
            }
            catch (InterruptedException ex) {
                logger.warn("Interrupted exception while dispatching messages to clients", ex);
                break;
            }
        }
    }

}




