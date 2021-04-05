package org.queasy.core.managed;

import com.google.common.collect.ImmutableList;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.queasy.ServerConfiguration;
import org.queasy.core.NoRunTestApplication;
import org.queasy.core.config.WriterConfiguration;
import org.queasy.core.util.Snowflake;
import org.queasy.db.QDbWriter;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class QueueWriterTest {

    private Jdbi jdbi;
    private QDbWriter qDbWriter;
    private QueueWriter qw;

    private static Snowflake idGenerator;

    private static DropwizardAppExtension<ServerConfiguration> app = new DropwizardAppExtension<>(
            NoRunTestApplication.class,
            ResourceHelpers.resourceFilePath("test_config.yml")
    );

    @BeforeAll
    public static void init() throws Exception {
        final String configPath = ResourceHelpers.resourceFilePath("test_config.yml");
        app.getApplication().run("db", "migrate", configPath);
        idGenerator = new Snowflake(0);
    }

    @BeforeEach
    public void setup() {
        jdbi = Jdbi.create("jdbc:sqlite:unit-test.db");
        jdbi.withHandle(handle -> handle.createUpdate("delete from queasy_q").execute());

        final WriterConfiguration writerConfig = new WriterConfiguration();
        writerConfig.setInsertBatchSize(4);
        writerConfig.setTableName("queasy_q");
        writerConfig.setRingBufferSize(16);

        qDbWriter = new QDbWriter(idGenerator, jdbi, writerConfig);
        qw = new QueueWriter(writerConfig, qDbWriter);
    }

    private List<String> makeMesgsList(List<Map<String, Object>> results) {
        return results.stream().map(row -> row.get("mesg").toString()).collect(Collectors.toList());
    }

    @Test
    public void testHappyPath() throws Exception {
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

        assertEquals(ImmutableList.of("m1","m2","m3","m4","m5","m6"), makeMesgsList(results));
    }

    @Test
    public void testWithDelayInBetween() throws Exception {
        qw.start();
        qw.publish(new String[]{"q1", "m1"});
        qw.publish(new String[]{"q1", "m2"});
        qw.publish(new String[]{"q2", "m3"});
        Thread.sleep(1000);

        List<Map<String, Object>> results = jdbi.withHandle(handle -> handle
                .createQuery("select id, qname, mesg from queasy_q order by id")
                .mapToMap()
                .list());

        assertEquals(ImmutableList.of("m1","m2","m3"), makeMesgsList(results));

        qw.publish(new String[]{"q2", "m4"});
        qw.publish(new String[]{"q1", "m5"});
        qw.publish(new String[]{"q1", "m6"});
        Thread.sleep(1000);
        qw.stop();
        qw.join();

        results = jdbi.withHandle(handle -> handle
                .createQuery("select id, qname, mesg from queasy_q order by id")
                .mapToMap()
                .list());

        assertEquals(ImmutableList.of("m1","m2","m3","m4","m5","m6"), makeMesgsList(results));
    }

    @AfterAll
    public static void destroy() {
        File file = new File(app.getConfiguration().getDatabase().getUrl().split(":")[2]);
        if (file.exists()) file.delete();
    }
}
