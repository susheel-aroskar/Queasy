package org.queasy.integration;

import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.queasy.ServerApplication;
import org.queasy.ServerConfiguration;
import org.queasy.client.ConsumerGroupMember;
import org.queasy.client.QueasyClient;
import org.queasy.client.QueueProducer;
import org.queasy.client.TopicSubscriber;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author saroskar
 * Created on: 2021-04-06
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class IntegrationTest {

    private static QueasyClient queasyClient;

    private static DropwizardAppExtension<ServerConfiguration> app = new DropwizardAppExtension<>(
            ServerApplication.class,
            ResourceHelpers.resourceFilePath("test_config.yml")
    );

    @BeforeAll
    public static void init() throws Exception {
        queasyClient = new QueasyClient();
        queasyClient.start();
        final String configPath = ResourceHelpers.resourceFilePath("test_config.yml");
        app.getApplication().run("db", "migrate", configPath);
    }

    private List<Thread> startThreads(List<Runnable> runnables) {
        final List<Thread> threads = new ArrayList<>(runnables.size());
        for (Runnable r : runnables) {
            Thread t = new Thread(r);
            t.start();
            threads.add(t);
        }
        return threads;
    }

    private void joinThreads(List<Thread> threads) throws Exception {
        for (Thread t : threads) {
            t.join();
        }
    }

    @Test
    public void test() throws Exception {
        final List<Runnable> runnables = new ArrayList<>();

        runnables.add(IntegrationTest.Producer.build("testQ1", 10));
        runnables.add(IntegrationTest.Producer.build("testQ2", 20));
        runnables.add(IntegrationTest.Producer.build("testQ1", 25));

        runnables.add(Consumer.build("testQ1-CG1"));
        runnables.add(Consumer.build("testQ1-CG1"));
        runnables.add(Consumer.build("testQ1-CG1"));

        runnables.add(Consumer.build("testQ2-CG1"));
        runnables.add(Consumer.build("testQ2-CG1"));

        runnables.add(Consumer.build("testQ1-CG2"));

        Subscriber.build("topic1-1");
        Subscriber.build("topic1-1");
        Subscriber.build("topic1-1");

        Subscriber.build("topic2-1");
        Subscriber.build("topic2-1");

        Subscriber.build("topic1-2");
        Subscriber.build("topic1-2");

        joinThreads(startThreads(runnables));
    }

    @AfterAll
    public static void destroy() throws Exception {
        queasyClient.stop();
        File file = new File(app.getConfiguration().getDatabase().getUrl().split(":")[2]);
        if (file.exists()) file.delete();

    }


    private static class Producer implements Runnable {
        private final QueueProducer producer;
        private final String id;
        private final int numOfMesgs;
        private final Random random;

        private static int idSeq;

        public Producer(QueueProducer producer, int numOfMesgs) {
            this.producer = producer;
            this.id = "producer-" + (idSeq++);
            this.numOfMesgs = numOfMesgs;
            this.random = new Random();
        }

        private static Producer build(String qName, int numOfMesgs) throws Exception {
            QueueProducer producer = queasyClient.connectProducer("ws://127.0.0.1:8080/", qName);
            return new Producer(producer, numOfMesgs);
        }

        @Override
        public void run() {
            for (int i = 0; i < numOfMesgs; i++) {
                try {
                    Thread.sleep(random.nextInt(100));
                    producer.writeMessage(id + ": Message# " + i);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

        }
    }

    private static class Consumer implements Runnable {
        private final ConsumerGroupMember cgm;
        private final String id;

        private static int idSeq;

        public Consumer(ConsumerGroupMember cgm, String cgName) {
            this.cgm = cgm;
            this.id = cgName + "-" + (idSeq++);
        }

        private static Consumer build(String cgName) throws Exception {
            ConsumerGroupMember cgm = queasyClient.connectToConsumerGroup("ws://127.0.0.1:8080/", cgName);
            return new Consumer(cgm, cgName);
        }

        @Override
        public void run() {
            while (cgm.isConnected()) {
                try {
                    String mesg = cgm.readMessage();
                    System.out.println(id + " => " + mesg);
                } catch (Exception ex) {
                    System.err.println(id + " : " + ex.getMessage());
                    ex.printStackTrace();
                }
            }

        }
    }

    private static class Subscriber implements TopicSubscriber {
        private final String id;

        private static int idSeq;

        public Subscriber(String topicName) {
            this.id = topicName + "-" + (idSeq++);
        }

        private static void build(String topicName) throws Exception {
            Subscriber ts = new Subscriber(topicName);
            queasyClient.subscribeToTopic("ws://127.0.0.1:8080/", topicName, ts);
        }

        @Override
        public void onMessage(String message) {
            System.out.println(id + "=>" + message);
        }

        @Override
        public void onError(Throwable t) {
            System.err.println(id + " : " + t.getMessage());
            t.printStackTrace();
        }

        @Override
        public void onClose() {
            System.err.println(id + " : CLOSED!");
        }

        @Override
        public void onDroppedMessages() {
            System.err.println(id + " : Dropped messages!");
        }
    }

}
