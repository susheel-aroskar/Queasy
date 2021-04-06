package org.queasy.core.managed;

import io.dropwizard.lifecycle.Managed;
import org.queasy.core.network.TopicSubscription;
import org.queasy.db.QDbReader;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author saroskar
 * Created on: 2021-04-05
 */
public class Topic implements Managed, Runnable {

    private final QDbReader qDbReader;
    private final LinkedBlockingQueue<TopicSubscription> subscribers;
    private AtomicInteger totalSubscribers;
    private volatile ArrayList<String> messages;


    public Topic(final QDbReader qDbReader) {
        this.qDbReader = qDbReader;
        this.subscribers = new LinkedBlockingQueue<>();
        this.totalSubscribers = new AtomicInteger();
    }

    @Override
    public void start() {
        qDbReader.readLastCheckpoint();
//        qDbReader.loadNextBatchOfMessages(messages);
    }

    @Override
    public void stop() {
        qDbReader.saveCheckpoint();
    }

    public void subscribe(final TopicSubscription conn) {
        totalSubscribers.incrementAndGet();
        subscribers.offer(conn);
    }

    public void unsubscribe(final TopicSubscription conn) {
        totalSubscribers.decrementAndGet();
    }


    @Override
    public void run() {
        int sentMesgsCount = 0;
        int subscribersCount = 0;
        while(true) {
            try {

            }
            catch(Exception ex) {

            }
        }
    }
}
