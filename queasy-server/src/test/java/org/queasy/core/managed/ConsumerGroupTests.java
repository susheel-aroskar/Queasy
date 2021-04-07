package org.queasy.core.managed;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.queasy.ServerConfiguration;
import org.queasy.core.NoRunTestApplication;
import org.queasy.core.config.CacheConfiguration;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.WriterConfiguration;
import org.queasy.core.network.Command;
import org.queasy.core.network.ConsumerConnection;
import org.queasy.core.util.Snowflake;
import org.queasy.db.QDbReader;
import org.queasy.db.QDbWriter;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author saroskar
 * Created on: 2021-03-29
 */
@ExtendWith(DropwizardExtensionsSupport.class)
public class ConsumerGroupTests {

    private Jdbi jdbi;
    private WriterConfiguration writerConfig;
    private ConsumerGroupConfiguration cgConfig;
    private QDbWriter qDbWriter;
    private QDbReader qDbReader;
    private QueueWriter queueWriter;
    private Cache<Long, String> messageCache;
    private ConsumerGroup cg;

    private static Snowflake idGenerator;

    public static final String CG_NAME = "testCG";

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
    public void setUp() {
        jdbi = Jdbi.create("jdbc:sqlite:unit-test.db");
        writerConfig = app.getConfiguration().getWriterConfiguration();
        qDbWriter = new QDbWriter(idGenerator, jdbi, writerConfig);
        queueWriter = new QueueWriter(writerConfig, qDbWriter);
        final Map<String, ConsumerGroupConfiguration> qConfigs = app.getConfiguration().getConsumerGroups();
        cgConfig = qConfigs.get(CG_NAME);
        jdbi.useHandle(handle -> handle.execute("delete from queasy_q"));
        jdbi.useHandle(handle -> handle.execute("delete from queasy_checkpoint"));
        final CacheConfiguration cc = app.getConfiguration().getCacheConfiguration();
        messageCache = Caffeine.newBuilder()
                .initialCapacity(cc.getInitialCapacity())
                .maximumSize(cc.getMaxSize())
                .expireAfterWrite(cc.getExpireAfter().toMilliseconds(), TimeUnit.MILLISECONDS)
                .build();

        qDbReader = new QDbReader(qDbWriter, jdbi, writerConfig, CG_NAME, cgConfig, messageCache);
        cg = new ConsumerGroup(qDbReader);
    }

    private Long readCheckPointFromDb() {
        return jdbi.withHandle(handle ->
                handle.select("select checkpoint from queasy_checkpoint where cg_name = ?", CG_NAME)
                        .map((rs, col, ctx) -> rs.getLong(col))
                        .first());
    }


    private String[] makeMessage(String queue, String mesg) {
        return new String[]{queue, mesg};
    }

    private String[] makeMessage(String mesg) {
        return makeMessage("testQ", mesg);
    }

    private ConsumerConnection makeConsumerConn(ConsumerGroup cg) {
        final ConsumerConnection c = new ConsumerConnection(cg);
        final ConsumerConnection conn = Mockito.spy(c);
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn).getRemote();
        return conn;
    }

    @Test
    public void testDefaultCheckpointOnInit() {
        cg.start();
        assertEquals(0L, qDbReader.getLastReadMessageId());
        assertEquals(0L, qDbReader.getReadBatchId());
    }

    @Test
    public void testLoadCheckpointFromDBOnInit() {
        jdbi.useHandle(handle -> handle.execute("insert into queasy_checkpoint (cg_name, checkpoint, ts) values (?,?,?)",
                CG_NAME, 29, System.currentTimeMillis()));
        cg.start();
        assertEquals(29L, qDbReader.getLastReadMessageId());
        assertEquals(0L, qDbReader.getReadBatchId());
    }

    @Test
    public void testCheckpointFromProducerOnInit() throws Exception {
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);
        cg.start();
        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(0L, qDbReader.getReadBatchId());
        queueWriter.stop();
    }

    @Test
    public void testQueueClientsWhenEmpty() throws Exception {
        queueWriter.start();
        Thread.sleep(100);
        cg.start();
        ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText(Command.DEQUEUE.toString());
        assertEquals(0L, qDbReader.getLastReadMessageId());
        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(0L, qDbReader.getReadBatchId());
        queueWriter.stop();
    }

    @Test
    public void testServeSingleClientImmediatelyWhenNotEmpty() throws Exception {
        cg.start();
        assertEquals(0L, qDbReader.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.start();
        Thread.sleep(100);
        final long id1 = qDbWriter.getLastWrittenMessageId();
        queueWriter.publish(makeMessage("test_2"));
        Thread.sleep(100);
        final long id2 = qDbWriter.getLastWrittenMessageId();
        ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();
        Mockito.verify(conn).sendMessage(QDbReader.buildMessage(id1 ,"test_1"));
        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());
        assertEquals(Collections.emptyList(), cg.getClients());
        assertEquals(ImmutableList.of(QDbReader.buildMessage(id2 ,"test_2")), cg.getMessages());
        assertEquals(1L, qDbReader.getReadBatchId());
        queueWriter.stop();
    }

    @Test
    public void testServeClientsImmediatelyWhenNotEmpty() throws Exception {
        cg.start();
        assertEquals(0L, qDbReader.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);
        ConsumerConnection conn1 = makeConsumerConn(cg);
        conn1.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();
        Mockito.verify(conn1).sendMessage(Mockito.endsWith("test_1}"));

        ConsumerConnection conn2 = makeConsumerConn(cg);
        conn2.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn2).sendMessage(QDbReader.buildMessage(qDbWriter.getLastWrittenMessageId(), "test_2"));

        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());
        assertEquals(Collections.emptyList(), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(1L, qDbReader.getReadBatchId());
        queueWriter.stop();
    }

    @Test
    public void testClientsShareMessageInstancesWithCache() throws Exception {
        final ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        final QDbReader qDbReader2 = new QDbReader(qDbWriter, jdbi, writerConfig, CG_NAME, cgConfig, messageCache);
        final ConsumerGroup cg2 = new ConsumerGroup(qDbReader2);


        cg.start();
        cg2.start();

        assertEquals(0L, qDbReader.getLastReadMessageId());
        assertEquals(0L, qDbReader2.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);
        ConsumerConnection conn1 = makeConsumerConn(cg);
        ConsumerConnection connB1 = makeConsumerConn(cg2);
        conn1.onWebSocketText(Command.DEQUEUE.toString());
        connB1.onWebSocketText(Command.DEQUEUE.toString());

        cg.run();
        cg2.run();

        Mockito.verify(conn1).sendMessage(messageCaptor.capture());
        Mockito.verify(connB1).sendMessage(messageCaptor.capture());
        List<String> mesgs = messageCaptor.getAllValues();
        assertEquals(2, mesgs.size());
        assertTrue(mesgs.get(0) == mesgs.get(1));

        ConsumerConnection conn2 = makeConsumerConn(cg);
        ConsumerConnection connB2 = makeConsumerConn(cg2);
        conn2.onWebSocketText(Command.DEQUEUE.toString());
        connB2.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn2).sendMessage(messageCaptor.capture());
        Mockito.verify(connB2).sendMessage(messageCaptor.capture());
        mesgs = messageCaptor.getAllValues();
        assertEquals(4, mesgs.size());
        assertTrue(mesgs.get(2) == mesgs.get(3));


        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());
        assertEquals(Collections.emptyList(), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(1L, qDbReader.getReadBatchId());
        queueWriter.stop();
    }

    @Test
    public void testClientsDoNotShareMessageInstancesWithoutCache() throws Exception {
        final QDbReader qDbReader2 = new QDbReader(qDbWriter, jdbi, writerConfig, CG_NAME, cgConfig, null);
        final ConsumerGroup cg2 = new ConsumerGroup(qDbReader2);

        final ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);


        cg.start();
        cg2.start();

        assertEquals(0L, qDbReader.getLastReadMessageId());
        assertEquals(0L, qDbReader2.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);
        ConsumerConnection conn1 = makeConsumerConn(cg);
        ConsumerConnection connB1 = makeConsumerConn(cg2);
        conn1.onWebSocketText(Command.DEQUEUE.toString());
        connB1.onWebSocketText(Command.DEQUEUE.toString());

        cg.run();
        cg2.run();

        Mockito.verify(conn1).sendMessage(messageCaptor.capture());
        Mockito.verify(connB1).sendMessage(messageCaptor.capture());
        List<String> mesgs = messageCaptor.getAllValues();
        assertEquals(2, mesgs.size());
        assertTrue(mesgs.get(0) != mesgs.get(1));

        ConsumerConnection conn2 = makeConsumerConn(cg);
        ConsumerConnection connB2 = makeConsumerConn(cg2);
        conn2.onWebSocketText(Command.DEQUEUE.toString());
        connB2.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn2).sendMessage(messageCaptor.capture());
        Mockito.verify(connB2).sendMessage(messageCaptor.capture());
        mesgs = messageCaptor.getAllValues();
        assertEquals(4, mesgs.size());
        assertTrue(mesgs.get(2) != mesgs.get(3));


        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());
        assertEquals(Collections.emptyList(), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(1L, qDbReader.getReadBatchId());
        queueWriter.stop();
    }

    @Test
    public void testSaveCheckpoint() throws Exception {
        cg.start();
        assertEquals(0L, qDbReader.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.publish(makeMessage("test_3"));
        queueWriter.start();
        Thread.sleep(100);
        ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();
        assertEquals(1L, qDbReader.getReadBatchId());
        Mockito.verify(conn).sendMessage(Mockito.endsWith("test_1}"));
        conn.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn).sendMessage(Mockito.endsWith("test_2}"));
        conn.onWebSocketText(Command.DEQUEUE.toString());
        final long secondMesgId = qDbReader.getLastReadMessageId();
        cg.run(); // fetchBatchSize: 2 in config. Need to fetch again from the DB
        assertEquals(secondMesgId, readCheckPointFromDb());
        assertEquals(2L, qDbReader.getReadBatchId());
        Mockito.verify(conn).sendMessage(QDbReader.buildMessage(qDbWriter.getLastWrittenMessageId(), "test_3"));

        conn.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();
        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(qDbWriter.getLastWrittenMessageId(), readCheckPointFromDb());
        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());
        assertEquals(2L, qDbReader.getReadBatchId());

        queueWriter.stop();
    }

    @Test
    public void testBailOutWhenNoClients() throws Exception {
        cg.start();
        assertEquals(0L, qDbReader.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.publish(makeMessage("test_3"));
        queueWriter.start();
        Thread.sleep(100);
        cg.run();
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(0L, readCheckPointFromDb());
        assertEquals(0L, qDbReader.getReadBatchId());
        queueWriter.stop();
    }

    @Test
    public void testBailOutWhenNoMoreMessagesFromProducer() throws Exception {
        cg.start();
        assertEquals(0L, qDbReader.getLastReadMessageId());
        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);

        final ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();
        assertEquals(1L, qDbReader.getReadBatchId());
        assertEquals(ImmutableList.of(QDbReader.buildMessage(qDbWriter.getLastWrittenMessageId(), "test_2"))
                , cg.getMessages());

        conn.onWebSocketText(Command.DEQUEUE.toString());
        conn.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();

        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(qDbWriter.getLastWrittenMessageId(), readCheckPointFromDb());
        assertEquals(1L, qDbReader.getReadBatchId());
        queueWriter.stop();
    }

    @Test
    public void testLoadNewMessagesWhenProducerAdvances() throws Exception {
        cg.start();
        final ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();
        assertEquals(0L, qDbReader.getReadBatchId());
        assertEquals(0L, qDbReader.getLastReadMessageId());

        queueWriter.publish(makeMessage("test_1"));
        queueWriter.start();
        Thread.sleep(100);
        final long id1 = qDbWriter.getLastWrittenMessageId();
        queueWriter.publish(makeMessage("test_2"));
        Thread.sleep(100);
        final long id2 = qDbWriter.getLastWrittenMessageId();


        cg.run();
        Mockito.verify(conn).sendMessage(QDbReader.buildMessage(id1,"test_1"));
        assertEquals(ImmutableList.of(QDbReader.buildMessage(id2,"test_2")), cg.getMessages());
        assertEquals(1L, qDbReader.getReadBatchId());
        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());

        conn.onWebSocketText(Command.DEQUEUE.toString());
        conn.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();
        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(1L, qDbReader.getReadBatchId());
        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());
        queueWriter.stop();
    }


    private void testCheckpointAdvancesWithNoMatchingMessagePublish() throws Exception {
        cg.start();
        final ConsumerConnection conn = makeConsumerConn(cg);
        conn.onWebSocketText(Command.DEQUEUE.toString());
        cg.run();
        assertEquals(0L, qDbReader.getReadBatchId());
        assertEquals(0L, qDbReader.getLastReadMessageId());

        queueWriter.publish(makeMessage("test_1"));
        queueWriter.publish(makeMessage("test_2"));
        queueWriter.start();
        Thread.sleep(100);
        long id = qDbWriter.getLastWrittenMessageId();

        cg.run();
        assertEquals(ImmutableList.of(QDbReader.buildMessage(id ,"test_2")), cg.getMessages());
        assertEquals(1L, qDbReader.getReadBatchId());
        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());

        conn.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn).sendMessage(QDbReader.buildMessage(id, "test_2"));
        conn.onWebSocketText(Command.DEQUEUE.toString());

        //publish messages to wrong queue
        queueWriter.publish(makeMessage("someQ", "test_3"));
        Thread.sleep(100);

        cg.run();
        assertEquals(ImmutableList.of(conn), cg.getClients());
        assertEquals(Collections.emptyList(), cg.getMessages());
        assertEquals(1L, qDbReader.getReadBatchId());
        assertEquals(qDbWriter.getLastWrittenMessageId(), qDbReader.getLastReadMessageId());
        assertEquals(qDbWriter.getLastWrittenMessageId(), readCheckPointFromDb());
        queueWriter.stop();
    }

    @Test
    public void testCheckpointAdvancesWithNoMatchingMessagePublishWithCache() throws Exception {
        testCheckpointAdvancesWithNoMatchingMessagePublish();
    }

    @Test
    public void testCheckpointAdvancesWithNoMatchingMessagePublishWithoutCache() throws Exception {
        qDbReader = new QDbReader(qDbWriter, jdbi, writerConfig, CG_NAME, cgConfig, null);
        cg = new ConsumerGroup(qDbReader);
        testCheckpointAdvancesWithNoMatchingMessagePublish();
    }

    @AfterAll
    public static void destroy() {
        File file = new File(app.getConfiguration().getDatabase().getUrl().split(":")[2]);
        if (file.exists()) file.delete();
    }

}
