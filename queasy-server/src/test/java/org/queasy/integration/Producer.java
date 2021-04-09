package org.queasy.integration;

import org.queasy.client.QueasyClient;
import org.queasy.client.QueueProducer;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * @author saroskar
 * Created on: 2021-04-08
 */
class Producer implements Runnable {

    private final int numOfMesgs;
    private volatile Set<String> messages;
    private final QueueProducer producer;
    private final Random random;
    private final String producerId;


    private static int idSeq;

    public Producer(QueasyClient client, String qName, int numOfMesgs) throws Exception {
        this.numOfMesgs = numOfMesgs;
        this.messages = new HashSet<>();
        this.producer = client.connectProducer("ws://127.0.0.1:8080/", qName);
        this.random = new Random();
        this.producerId = "producer-" + (idSeq++);
    }

    public Set<String> getMessages() {
        return messages;
    }

    public void publish(String mesg) throws Exception {
        producer.writeMessage(mesg);
    }

    public String makeMessage(final String text) {
        return String.format("{\"producerId\" : \"%s\", \"body\": \"%s\"}", producerId, text);
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < numOfMesgs; i++) {
//                Thread.sleep(random.nextInt(50));
                final Integer rnd = random.nextInt();
                final String text = "TEXT:" + rnd;
                final String mesg = makeMessage(text);
                publish(mesg);
                messages.add(text);
            }
        } catch (TimeoutException ex) {
            return;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
