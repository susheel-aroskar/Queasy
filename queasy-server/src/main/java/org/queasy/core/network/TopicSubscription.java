package org.queasy.core.network;

import org.eclipse.jetty.websocket.api.Session;
import org.queasy.core.managed.Topic;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author saroskar
 * Created on: 2021-04-05
 */
public class TopicSubscription extends BaseWebSocketConnection {

    private final Topic topic;
    private final AtomicInteger pendingMesgsCount;

    private volatile long readBatchId;
    private volatile int currentMesgIndex;

    public TopicSubscription(Topic topic) {
        this.topic = topic;
        pendingMesgsCount = new AtomicInteger();
    }

    public int getPendingMesgsCount() {
        return pendingMesgsCount.get();
    }

    public int getCurrentMesgIndex() {
        return currentMesgIndex;
    }

    @Override
    public void writeSuccess() {
        pendingMesgsCount.decrementAndGet();
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        super.onWebSocketConnect(sess);
        topic.subscribe(this);
    }

    public void sendMessage(final long readBatchId, final int mesgIndex, final String message) {
        this.readBatchId = readBatchId;
        this.currentMesgIndex = mesgIndex;
        pendingMesgsCount.incrementAndGet();
        writeMessage(message);
    }

}
