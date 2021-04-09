package org.queasy.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.queasy.client.ConsumerGroupMember;
import org.queasy.client.QueasyClient;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

/**
 * @author saroskar
 * Created on: 2021-04-08
 */
class Consumer implements Runnable {

    private volatile Set<String> messages;
    private final ConsumerGroupMember cgm;
    private final String consumerId;
    private final CountDownLatch latch;


    private static int idSeq;
    private static final ObjectMapper mapper = new ObjectMapper();

    public Consumer(QueasyClient client, String cgName, CountDownLatch latch) throws Exception {
        this.messages = new HashSet<>();
        this.cgm = client.connectToConsumerGroup("ws://127.0.0.1:8080/", cgName);
        this.consumerId = "CGM-" + (idSeq++);
        this.latch = latch;
    }

    public Set<String> getMessages() {
        return messages;
    }

    @Override
    public void run() {
        while (cgm.isConnected()) {
            try {
                final String mesg = cgm.readMessage();
                final Map result = mapper.readValue(mesg, HashMap.class);
                final Map m = (Map) result.get("message");
                final String text = m.get("body").toString();
                if (!"END".equals(text)) {
                    messages.add(text);
                    System.out.println(consumerId + " => " + mesg);
                } else {
                    latch.countDown();
                    System.err.println(consumerId + " => " + mesg);
                }
            } catch (TimeoutException ex) {
                return;
            } catch (Exception ex) {
                System.err.println(consumerId + " : " + ex.getMessage());
            }
        }
    }
}
