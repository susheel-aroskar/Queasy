package org.queasy.integration;

import com.google.common.collect.Lists;
import io.dropwizard.cli.CheckCommand;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.queasy.ServerConfiguration;
import org.queasy.client.QueasyClient;
import org.queasy.core.NoRunTestApplication;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author saroskar
 * Created on: 2021-04-06
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class IntegrationTest {

    private static QueasyClient queasyClient;

    private static DropwizardAppExtension<ServerConfiguration> app = new DropwizardAppExtension<>(
            NoRunTestApplication.class,
            ResourceHelpers.resourceFilePath("test_config.yml"),
            "", CheckCommand::new
    );

    @BeforeAll
    public static void init() throws Exception {
        final File db = new File(app.getConfiguration().getDatabase().getUrl().split(":")[2]);
        if (db.exists()) db.delete(); //drop old DB, if one is left behind from previous run

        queasyClient = new QueasyClient();
        queasyClient.start();
        final String configPath = ResourceHelpers.resourceFilePath("test_config.yml");
        app.manage(new Managed() {
            @Override
            public void start() {
            }

            @Override
            public void stop() {
                File file = new File(app.getConfiguration().getDatabase().getUrl().split(":")[2]);
                if (file.exists()) file.delete(); // Drop database on stop
            }
        });

        app.getApplication().run("db", "migrate", configPath); // Initialize new database on start up
        app.getApplication().run("server", configPath); // Run the real application
    }

    private List<Thread> startThreads(Runnable... runnables) {
        final List<Thread> threads = new ArrayList<>(runnables.length);
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

    private HashSet<String> mergeNonOverlappingSet(Set<String>... mesgSets) {
        final HashSet<String> mesgs = new HashSet<>();
        for (final Set<String> mesgSet : mesgSets) {
            for (final String mesg : mesgSet) {
                if (!mesgs.add(mesg)) {
                    return null; //overlap (duplicate message received) found!
                }
            }
        }
        return mesgs;
    }

    @Test
    public void test() throws Exception {
        final Random random = new Random();
        final CountDownLatch latch = new CountDownLatch(1 + 1 + 1 + 3 + 2 + 2); // 1 for each consumer group + 1 for each subscriber

        final int p1MesgCount = random.nextInt(229);
        final Producer p1 = new Producer(queasyClient, "testQ1", p1MesgCount);
        final int p2MesgCount = random.nextInt(113);
        final Producer p2 = new Producer(queasyClient, "testQ2", random.nextInt(p2MesgCount));
        final int p3MesgCount = random.nextInt(73);
        final Producer p3 = new Producer(queasyClient, "testQ1", random.nextInt(p3MesgCount));

        final Consumer q1cg1_2 = new Consumer(queasyClient, "testQ1-CG1", latch);
        final Consumer q1cg1_1 = new Consumer(queasyClient, "testQ1-CG1", latch);
        final Consumer q1cg1_3 = new Consumer(queasyClient, "testQ1-CG1", latch);

        final Consumer q2cg1_1 = new Consumer(queasyClient, "testQ2-CG1", latch);
        final Consumer q2cg1_2 = new Consumer(queasyClient, "testQ2-CG1", latch);

        final Consumer q1cg2_1 = new Consumer(queasyClient, "testQ1-CG2", latch);

        final Subscriber topic11S1 = new Subscriber(queasyClient, "topic1-1", latch);
        final Subscriber topic11S2 = new Subscriber(queasyClient, "topic1-1", latch);
        final Subscriber topic11S3 = new Subscriber(queasyClient, "topic1-1", latch);

        final Subscriber topic21S1 = new Subscriber(queasyClient, "topic2-1", latch);
        final Subscriber topic21S2 = new Subscriber(queasyClient, "topic2-1", latch);

        final Subscriber topic12S1 = new Subscriber(queasyClient, "topic1-2", latch);
        final Subscriber topic12S2 = new Subscriber(queasyClient, "topic1-2", latch);


        final List<List<Thread>> threadGroups = Lists.partition(startThreads(p1, p2, p3, q1cg1_1, q1cg1_2, q1cg1_3, q2cg1_1, q2cg1_2, q1cg2_1), 3);
        joinThreads(threadGroups.get(0));
        p2.publish(p2.makeMessage("END"));
        p3.publish(p3.makeMessage("END"));

        final long start = System.currentTimeMillis();
        latch.await();
        final long stop = System.currentTimeMillis();
        System.err.println("Published " + (p1MesgCount+p2MesgCount+p3MesgCount)+" messages.");
        System.err.println("Finished in " + (stop - start) + " ms after last message was published.");
        queasyClient.stop();

        final Set<String> producedMesgsQ1 = mergeNonOverlappingSet(p1.getMessages(), p3.getMessages());
        final Set<String> cg11Mesgs = mergeNonOverlappingSet(q1cg1_1.getMessages(), q1cg1_2.getMessages(), q1cg1_3.getMessages());
        assertNotNull(cg11Mesgs, "Consumer group testQ1-CG1 received duplicate messages");
        assertEquals(producedMesgsQ1, cg11Mesgs);

        final Set<String> producedMesgsQ2 = p2.getMessages();
        final Set<String> cg21Mesgs = mergeNonOverlappingSet(q2cg1_1.getMessages(), q2cg1_2.getMessages());
        assertNotNull(cg21Mesgs, "Consumer group testQ2-CG1 received duplicate messages");
        assertEquals(producedMesgsQ2, cg21Mesgs);

        final Set<String> cg12Mesgs = mergeNonOverlappingSet(q1cg2_1.getMessages());
        assertNotNull(cg11Mesgs, "Consumer group testQ1-CG2 received duplicate messages");
        assertEquals(producedMesgsQ1, cg12Mesgs);

        assertEquals(producedMesgsQ1, topic11S1.getMessages());
        assertEquals(producedMesgsQ1, topic11S2.getMessages());
        assertEquals(producedMesgsQ1, topic11S3.getMessages());

        assertEquals(producedMesgsQ2, topic21S1.getMessages());
        assertEquals(producedMesgsQ2, topic21S2.getMessages());

        assertEquals(producedMesgsQ1, topic12S1.getMessages());
        assertEquals(producedMesgsQ1, topic12S2.getMessages());
    }

}
