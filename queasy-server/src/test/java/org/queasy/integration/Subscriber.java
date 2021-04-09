package org.queasy.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.queasy.client.QueasyClient;
import org.queasy.client.TopicSubscriber;
import org.queasy.client.TopicSubscription;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * @author saroskar
 * Created on: 2021-04-08
 */
class Subscriber implements TopicSubscriber {

    private volatile Set<String> messages;
    private final String subscriberId;
    private final TopicSubscription subscription;
    final CountDownLatch latch;


    private static int idSeq;
    private static final ObjectMapper mapper = new ObjectMapper();

    public Subscriber(QueasyClient client, String topicName, CountDownLatch latch) throws Exception {
        this.messages = new HashSet<>();
        this.subscription = client.subscribeToTopic("ws://127.0.0.1:8080/", topicName, this);
        this.subscriberId = topicName + "-" + (idSeq++);
        this.latch = latch;

    }

    public synchronized Set<String> getMessages() {
        return messages;
    }

    @Override
    public void onMessage(String message) {
        try {
            final Map result = mapper.readValue(message, HashMap.class);
            final Map m = (Map)result.get("message");
            final String text = m.get("body").toString();
            if ("END".equals(text)) {
                synchronized (this) {
                    latch.countDown();
                }
                System.err.println(subscriberId + "=>" + message);
            } else {
                System.out.println(subscriberId + "=>" + message);
                messages.add(text);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void onError(Throwable t) {
        System.err.println(subscriberId + " : " + t.getMessage());
        t.printStackTrace();
    }

    @Override
    public void onClose() {
        System.err.println(subscriberId + " : CLOSED!");
    }

    @Override
    public void onDroppedMessages() {
        System.err.println(subscriberId + " : Dropped messages!");
    }
}
