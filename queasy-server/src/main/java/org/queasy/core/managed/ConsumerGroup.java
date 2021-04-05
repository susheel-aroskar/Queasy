package org.queasy.core.managed;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.util.Strings;
import org.jdbi.v3.core.Jdbi;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.QueueConfiguration;
import org.queasy.core.network.ConsumerConnection;
import org.queasy.core.network.Status;
import org.queasy.db.QDbReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ConsumerGroup implements Managed, Runnable {

    private final QDbReader qDbReader;
    private final ArrayBlockingQueue<String> messages;
    private final LinkedBlockingQueue<ConsumerConnection> clients;

    private static final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class);


    public ConsumerGroup(final QDbReader qDbReader) {
        this.qDbReader = qDbReader;
        this.messages = new ArrayBlockingQueue<>(qDbReader.getFetchSize() + 1);
        this.clients = new LinkedBlockingQueue<>();
    }

    @Override
    public void start() {
        qDbReader.readLastCheckpoint();
    }

    @Override
    public void stop() {
        qDbReader.saveCheckpoint();
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

                if (client.isTimedOut(qDbReader.getTimeout())) {
                    client.sendMessage(Status.TIMEOUT.toString());
                    continue;
                }

                // we have run out of fetched messages, add the client back to wait queue
                clients.put(client);

                //Try to load more messages from DB
                if (!qDbReader.loadNextBatchOfMessages(messages)) {
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


    /* Package private methods, visible only for and to unit tests */


    @VisibleForTesting
    ConsumerGroup(String... messages) {
        //Used exclusively for creating mocks in tests
        this.qDbReader = null;
        this.messages = new ArrayBlockingQueue<>(messages.length+1);
        this.messages.addAll(Arrays.asList(messages));
        this.clients = new LinkedBlockingQueue<>();
    }

    @VisibleForTesting
    List<String> getMessages() {
        return messages.stream().collect(Collectors.toList());
    }

    @VisibleForTesting
    List<ConsumerConnection> getClients() {
        return clients.stream().collect(Collectors.toList());
    }

}




