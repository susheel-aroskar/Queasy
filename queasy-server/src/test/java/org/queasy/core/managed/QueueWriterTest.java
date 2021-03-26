package org.queasy.core.managed;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.queasy.ServerApplication;
import org.queasy.ServerConfiguration;
import org.queasy.core.config.QueueConfiguration;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class QueueWriterTest {

    private static DropwizardAppExtension<ServerConfiguration> app = new DropwizardAppExtension<>(
            ServerApplication.class,
            ResourceHelpers.resourceFilePath("test_config.yml")
    );

    @BeforeAll
    public static void setup() throws Exception {
        final String configPath = ResourceHelpers.resourceFilePath("test_config.yml");
        app.getApplication().run("db", "migrate", configPath);
    }

    @Test
    public void testHappyPath() throws Exception {
        final Jdbi jdbi = Jdbi.create("jdbc:sqlite:unit-test.db");
        jdbi.withHandle(handle -> handle.createUpdate("delete from queasy_q").execute());

        final QueueConfiguration qConfig = new QueueConfiguration();
        qConfig.setInsertBatchSize(4);
        qConfig.setTableName("queasy_q");
        qConfig.setRingBufferSize(16);
        QueueWriter qw = new QueueWriter(qConfig, jdbi);

        qw.start();
        qw.publish(new String[]{"q1", "m1"});
        qw.publish(new String[]{"q1", "m2"});
        qw.publish(new String[]{"q2", "m3"});
        qw.publish(new String[]{"q2", "m4"});
        qw.publish(new String[]{"q1", "m5"});
        qw.publish(new String[]{"q1", "m6"});
        Thread.sleep(1000);
        qw.stop();
        qw.join();

        List<Map<String, Object>> results = jdbi.withHandle(handle -> handle
                .createQuery("select id, qname, mesg from queasy_q order by id")
                .mapToMap()
                .list());

        assertEquals(ImmutableList.of(
                ImmutableMap.of("id", 1, "qname", "q1", "mesg", "m1"),
                ImmutableMap.of("id", 2, "qname", "q1", "mesg", "m2"),
                ImmutableMap.of("id", 3, "qname", "q2", "mesg", "m3"),
                ImmutableMap.of("id", 4, "qname", "q2", "mesg", "m4"),
                ImmutableMap.of("id", 5, "qname", "q1", "mesg", "m5"),
                ImmutableMap.of("id", 6, "qname", "q1", "mesg", "m6")
                ), results
        );

        assertEquals(6, qw.getCurrentId());
    }

    @Test
    public void testWithDelayInBetween() throws Exception {
        final Jdbi jdbi = Jdbi.create("jdbc:sqlite:unit-test.db");
        jdbi.withHandle(handle -> handle.createUpdate("delete from queasy_q").execute());

        final QueueConfiguration qConfig = new QueueConfiguration();
        qConfig.setInsertBatchSize(4);
        qConfig.setTableName("queasy_q");
        qConfig.setRingBufferSize(16);
        QueueWriter qw = new QueueWriter(qConfig, jdbi);

        qw.start();
        qw.publish(new String[]{"q1", "m1"});
        qw.publish(new String[]{"q1", "m2"});
        qw.publish(new String[]{"q2", "m3"});
        Thread.sleep(1000);
        assertEquals(3, qw.getCurrentId());

        List<Map<String, Object>> results = jdbi.withHandle(handle -> handle
                .createQuery("select id, qname, mesg from queasy_q order by id")
                .mapToMap()
                .list());

        assertEquals(ImmutableList.of(
                ImmutableMap.of("id", 1, "qname", "q1", "mesg", "m1"),
                ImmutableMap.of("id", 2, "qname", "q1", "mesg", "m2"),
                ImmutableMap.of("id", 3, "qname", "q2", "mesg", "m3")
                ), results
        );

        qw.publish(new String[]{"q2", "m4"});
        qw.publish(new String[]{"q1", "m5"});
        qw.publish(new String[]{"q1", "m6"});
        Thread.sleep(1000);
        assertEquals(6, qw.getCurrentId());
        qw.stop();
        qw.join();

        results = jdbi.withHandle(handle -> handle
                .createQuery("select id, qname, mesg from queasy_q order by id")
                .mapToMap()
                .list());

        assertEquals(ImmutableList.of(
                ImmutableMap.of("id", 1, "qname", "q1", "mesg", "m1"),
                ImmutableMap.of("id", 2, "qname", "q1", "mesg", "m2"),
                ImmutableMap.of("id", 3, "qname", "q2", "mesg", "m3"),
                ImmutableMap.of("id", 4, "qname", "q2", "mesg", "m4"),
                ImmutableMap.of("id", 5, "qname", "q1", "mesg", "m5"),
                ImmutableMap.of("id", 6, "qname", "q1", "mesg", "m6")
                ), results
        );
    }

    @AfterAll
    public static void destroy() {
        File file = new File(app.getConfiguration().getDatabase().getUrl().split(":")[2]);
        if (file.exists()) file.delete();
    }
}
