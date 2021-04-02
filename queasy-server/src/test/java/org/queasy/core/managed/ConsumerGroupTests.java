package org.queasy.core.managed;

import com.google.common.collect.ImmutableList;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.queasy.ServerConfiguration;
import org.queasy.core.NoRunTestApplication;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.QueueConfiguration;
import org.queasy.core.network.ConsumerConnection;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author saroskar
 * Created on: 2021-03-29
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class ConsumerGroupTests {

    private Jdbi jdbi;
    private QueueConfiguration qConfig;
    private ConsumerGroupConfiguration cgConfig;
    private QueueWriter queueWriter;

    public static final String CG_NAME = "testCG";

    private static DropwizardAppExtension<ServerConfiguration> app = new DropwizardAppExtension<>(
            NoRunTestApplication.class,
            ResourceHelpers.resourceFilePath("test_config.yml")
    );

    @BeforeAll
    public static void init() throws Exception {
        final String configPath = ResourceHelpers.resourceFilePath("test_config.yml");
        app.getApplication().run("db", "migrate", configPath);
    }

    @BeforeEach
    public void setUp() {
        jdbi = Jdbi.create("jdbc:sqlite:unit-test.db");
        qConfig = app.getConfiguration().getQueue();
        queueWriter = new QueueWriter(qConfig, jdbi);
        final Map<String, ConsumerGroupConfiguration> cgConfigs = app.getConfiguration().getConsumerGroups();
        cgConfig = cgConfigs.get(CG_NAME);
        jdbi.useHandle(handle -> handle.execute("delete from queasy_q"));
        jdbi.useHandle(handle -> handle.execute("delete from queasy_checkpoint"));
    }

    private Long readCheckPointFromDb() {
        return jdbi.withHandle(handle ->
                handle.select("select checkpoint from queasy_checkpoint where cg_name = ?", CG_NAME)
                        .map((rs, col, ctx) -> rs.getLong(col))
                        .first());
    }

    private String[] makeMessage(String mesg) {
        return new String[]{cgConfig.getQueueName(), mesg};
    }

    private ConsumerConnection makeConsumerConn(ConsumerGroup cg) {
        final ConsumerConnection c = new ConsumerConnection(cg);
        final ConsumerConnection conn = Mockito.spy(c);
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn).getRemote();
        return conn;
    }

    @Test
    public void testDefaultCheckpointOnInit() {
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        assertEquals(0L, cg.getLastReadMessageId());
        assertEquals(0L, cg.getNumOfDbFetches());
    }

    @Test
    public void testLoadCheckpointFromDBOnInit() {
        jdbi.useHandle(handle -> handle.execute("insert into queasy_checkpoint (cg_name, checkpoint, ts) values (?,?,?)",
                CG_NAME, 29, System.currentTimeMillis()));
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        assertEquals(29L, cg.getLastReadMessageId());
        assertEquals(0L, cg.getNumOfDbFetches());
    }

    @Test
    public void testCheckpointFromProducerOnInit() throws Exception {
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        assertEquals(2L, cg.getLastReadMessageId());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(0L, cg.getNumOfDbFetches());
        queueWriter.stop();
    }

    @Test
    public void testQueueClientsWhenEmpty() throws Exception {
        queueWriter.start();
        Thread.sleep(100);
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText("#GET");
        assertEquals(0L, cg.getLastReadMessageId());
        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(0L, cg.getNumOfDbFetches());
        queueWriter.stop();
    }

    @Test
    public void testServeSingleClientImmediatelyWhenNotEmpty() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        assertEquals(0L, cg.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);
        ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText("#GET");
        cg.run();
        Mockito.verify(conn).sendMessage("test_1");
        assertEquals(2L, cg.getLastReadMessageId());
        assertEquals(Collections.emptyList(), cg.getClients());
        assertEquals(ImmutableList.of("test_2"), cg.getMessages());
        assertEquals(1L, cg.getNumOfDbFetches());
        queueWriter.stop();
    }

    @Test
    public void testServeClientsImmediatelyWhenNotEmpty() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        assertEquals(0L, cg.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);
        ConsumerConnection conn1 = makeConsumerConn(cg);
        conn1.onWebSocketText("#GET");
        cg.run();
        Mockito.verify(conn1).sendMessage("test_1");

        ConsumerConnection conn2 = makeConsumerConn(cg);
        conn2.onWebSocketText("#GET");
        Mockito.verify(conn2).sendMessage("test_2");

        assertEquals(2L, cg.getLastReadMessageId());
        assertEquals(Collections.emptyList(), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(1L, cg.getNumOfDbFetches());
        queueWriter.stop();
    }

    @Test
    public void testSaveCheckpoint() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        assertEquals(0L, cg.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.publish(makeMessage("test_3"));
        queueWriter.start();
        Thread.sleep(100);
        ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText("#GET");
        cg.run();
        assertEquals(1L, cg.getNumOfDbFetches());
        Mockito.verify(conn).sendMessage("test_1");
        conn.onWebSocketText("#GET");
        Mockito.verify(conn).sendMessage("test_2");
        conn.onWebSocketText("#GET");
        cg.run(); // selectBatchSize: 2 in config. Need to fetch again from the DB
        assertEquals(2L, cg.getNumOfDbFetches());
        Mockito.verify(conn).sendMessage("test_3");
        assertEquals(2L, readCheckPointFromDb());

        conn.onWebSocketText("#GET");
        cg.run();
        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(3L, readCheckPointFromDb());
        assertEquals(2L, cg.getNumOfDbFetches());

        queueWriter.stop();
    }

    @Test
    public void testBailOutWhenNoClients() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        assertEquals(0L, cg.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.publish(makeMessage("test_3"));
        queueWriter.start();
        Thread.sleep(100);
        cg.run();
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(0L, readCheckPointFromDb());
        assertEquals(0L, cg.getNumOfDbFetches());
        queueWriter.stop();
    }

    @Test
    public void testBailOutWhenNoMoreMessagesFromProducer() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        assertEquals(0L, cg.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);

        final ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText("#GET");
        cg.run();
        assertEquals(1L, cg.getNumOfDbFetches());
        assertEquals(ImmutableList.of("test_2"), cg.getMessages());

        conn.onWebSocketText("#GET");
        conn.onWebSocketText("#GET");
        cg.run();

        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(2L, readCheckPointFromDb());
        assertEquals(1L, cg.getNumOfDbFetches());
        queueWriter.stop();
    }

    @Test
    public void testLoadNewMessagesWhenProducerAdvances() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup(CG_NAME, cgConfig, qConfig, jdbi, queueWriter);
        final ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText("#GET");
        cg.run();
        assertEquals(0L, cg.getNumOfDbFetches());
        assertEquals(0L, cg.getLastReadMessageId());

        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);

        cg.run();
        assertEquals(ImmutableList.of("test_2"), cg.getMessages());
        assertEquals(1L, cg.getNumOfDbFetches());
        assertEquals(2L, cg.getLastReadMessageId());

        conn.onWebSocketText("#GET");
        conn.onWebSocketText("#GET");
        cg.run();
        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(1L, cg.getNumOfDbFetches());
        assertEquals(2L, cg.getLastReadMessageId());
        queueWriter.stop();
    }

    @AfterAll
    public static void destroy() {
        File file = new File(app.getConfiguration().getDatabase().getUrl().split(":")[2]);
        if (file.exists()) file.delete();
    }

}
