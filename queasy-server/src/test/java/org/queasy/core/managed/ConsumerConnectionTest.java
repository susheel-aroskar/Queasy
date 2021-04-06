package org.queasy.core.managed;

import com.google.common.collect.ImmutableList;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.queasy.core.network.Command;
import org.queasy.core.network.ConsumerConnection;
import org.queasy.db.QDbReader;

import java.util.Collections;

/**
 * @author saroskar
 * Created on: 2021-03-29
 */
public class ConsumerConnectionTest {


    @Test
    public void testGETWithNoMessage() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup();
        final ConsumerGroup testCG = Mockito.spy(cg);
        final ConsumerConnection conn = new ConsumerConnection(testCG);
        conn.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(testCG).waitForMessage(conn);
        assertEquals(ImmutableList.of(conn), cg.getClients());
    }

    @Test
    public void testMultipleGETsWithNoMessage() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup();
        final ConsumerGroup testCG = Mockito.spy(cg);
        final ConsumerConnection conn = new ConsumerConnection(testCG);
        conn.onWebSocketText(Command.DEQUEUE.toString());
        conn.onWebSocketText(Command.DEQUEUE.toString());
        conn.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(testCG, Mockito.times(1)).waitForMessage(conn); // Invoked only once
        assertEquals(ImmutableList.of(conn), cg.getClients());
    }

    @Test
    public void testGETWithOneMessage() {
        final ConsumerGroup cg = new ConsumerGroup("test_1");
        final ConsumerGroup testCG = Mockito.spy(cg);
        final ConsumerConnection c = new ConsumerConnection(testCG);
        final ConsumerConnection conn = Mockito.spy(c);
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn).getRemote();
        conn.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn).sendMessage("test_1");
        assertEquals(Collections.emptyList(), cg.getClients());
    }

    @Test
    public void testGETSWithOneMessage() {
        final ConsumerGroup cg = new ConsumerGroup("test_1");
        final ConsumerGroup testCG = Mockito.spy(cg);
        final ConsumerConnection c = new ConsumerConnection(testCG);
        final ConsumerConnection conn = Mockito.spy(c);
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn).getRemote();
        conn.onWebSocketText(Command.DEQUEUE.toString());
        assertEquals(Collections.emptyList(), cg.getClients());
        conn.onWebSocketText(Command.DEQUEUE.toString());
        assertEquals(ImmutableList.of(conn), cg.getClients());
        Mockito.verify(conn, Mockito.times(1)).sendMessage("test_1"); // only one invocation
    }

    @Test
    public void testGETWithTwoMessages() {
        final ConsumerGroup cg = new ConsumerGroup("test_1", "test_2");
        final ConsumerGroup testCG = Mockito.spy(cg);
        final ConsumerConnection c = new ConsumerConnection(testCG);
        final ConsumerConnection conn = Mockito.spy(c);
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn).getRemote();
        conn.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn).sendMessage("test_1");
        assertEquals(Collections.emptyList(), cg.getClients());
    }

    @Test
    public void testGETSWithTwoMessages() {
        final ConsumerGroup cg = new ConsumerGroup("test_1", "test_2");
        final ConsumerGroup testCG = Mockito.spy(cg);
        final ConsumerConnection c = new ConsumerConnection(testCG);
        final ConsumerConnection conn = Mockito.spy(c);
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn).getRemote();
        conn.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn).sendMessage("test_1");
        assertEquals(Collections.emptyList(), cg.getClients());
        conn.onWebSocketText(Command.DEQUEUE.toString());
        Mockito.verify(conn).sendMessage("test_2");
        assertEquals(Collections.emptyList(), cg.getClients());
    }

    @Test
    public void testTimeout() throws Exception {
        QDbReader qDbReader = Mockito.mock(QDbReader.class);
        Mockito.when(qDbReader.getFetchSize()).thenReturn(2);
        Mockito.when(qDbReader.getTimeout()).thenReturn(100L);
        final ConsumerGroup testCG = new ConsumerGroup(qDbReader);
        final ConsumerConnection conn1 = Mockito.spy(new ConsumerConnection(testCG));
        final ConsumerConnection conn2 = Mockito.spy(new ConsumerConnection(testCG));
        final ConsumerConnection conn3 = Mockito.spy(new ConsumerConnection(testCG));
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn1).getRemote();
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn2).getRemote();
        Mockito.doReturn(Mockito.mock(RemoteEndpoint.class)).when(conn3).getRemote();
        conn1.onWebSocketText(Command.DEQUEUE.toString());
        conn2.onWebSocketText(Command.DEQUEUE.toString());
        Thread.sleep(110);

        testCG.run();
        Mockito.verify(conn1).sendMessage(":TIMEOUT");
        Mockito.verify(conn2).sendMessage(":TIMEOUT");
        conn3.onWebSocketText(Command.DEQUEUE.toString());
        testCG.run();
        Mockito.verify(conn3, Mockito.times(0)).sendMessage(Mockito.anyString());
    }

}
