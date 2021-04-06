package org.queasy.core.managed;

import io.dropwizard.lifecycle.Managed;
import org.queasy.core.config.TopicConfiguration;
import org.queasy.core.network.TopicSubscription;
import org.queasy.db.QDbReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author saroskar
 * Created on: 2021-04-05
 */
public class Topic implements Managed, Runnable {

    private final int fetchBatchSize;
    private final int quorumPercentage;
    private final QDbReader qDbReader;
    private final LinkedBlockingQueue<TopicSubscription> subscribers;
    private AtomicInteger totalSubscribers;
    private volatile ArrayList<String> messages;

    private static final Logger logger = LoggerFactory.getLogger(Topic.class);


    public Topic(final TopicConfiguration config, final QDbReader qDbReader) {
        this.fetchBatchSize = config.getFetchBatchSize();
        this.quorumPercentage = config.getQuorumPercentage();
        this.qDbReader = qDbReader;
        this.subscribers = new LinkedBlockingQueue<>();
        this.totalSubscribers = new AtomicInteger();
    }

    @Override
    public void start() {
        qDbReader.readLastCheckpoint();
    }

    @Override
    public void stop() {
        qDbReader.saveCheckpoint();
    }

    public void subscribe(final TopicSubscription sub) {
        totalSubscribers.incrementAndGet();
        waitForMessages(sub);
    }

    public void unsubscribe(final TopicSubscription sub) {
        totalSubscribers.decrementAndGet();
    }

    /**
     * @param sub
     * @return false if messages are available for the given subscriber for immediate consumption, true if subscriber
     * must wait for new messages to become available.
     */
    public boolean waitForMessages(final TopicSubscription sub)  {
        if ((sub.getMessageBatchId() < qDbReader.getReadBatchId()) && (messages != null) && (!messages.isEmpty())) {
            sub.setNextMessageBatch(qDbReader.getReadBatchId(), messages);
            return false;
        } else {
            // This subscriber is waiting for next message batch, add it back to wait queue
            subscribers.offer(sub);
            return true;
        }
    }

    private boolean loadNextMessageBatch() {
        if (qDbReader.hasMoreMessages()) {
            messages = new ArrayList<>(fetchBatchSize);
            return qDbReader.loadNextBatchOfMessages(messages);
        }
        return false;
    }

    @Override
    public void run() {
        while(true) {
            try {
                final int subscriberCount = subscribers.size();
                final int totalSubs = totalSubscribers.get();

                if (subscriberCount == 0) {
                    break; // No subscribers waiting, bail out
                }

                if (messages == null || messages.isEmpty()) {
                    if (!loadNextMessageBatch()) {
                        break; //There are no more messages left, bail out
                    }
                }

                // Dispatch current message batch to waiting subscribers
                int waitingSubs = 0;
                for (int i = 0; i < subscriberCount; i++) {
                    final TopicSubscription sub = subscribers.poll();
                    if (sub != null && sub.isConnected()) {
                        if (waitForMessages(sub)) {
                            waitingSubs++;
                        }
                    }
                }

                if ((totalSubs > 0) && ((waitingSubs * 100 / totalSubs) >= quorumPercentage)) {
                    // Quorum reached for fetching the next message batch
                    if (loadNextMessageBatch()) {
                        continue; // New message batch loaded, continue processing
                    }
                }

                break; // No waiting clients OR no messages left OR quorum not reached to fetch next message batch
            }
            catch(Exception ex) {
                logger.warn("Exception while dispatching messages to subscribers", ex);
                break;
            }
        }
    }
}
