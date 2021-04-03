package org.queasy.core.managed;

import com.google.common.collect.ImmutableList;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.queasy.core.network.ConsumerConnection;

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
        conn.onWebSocketText("#GET");
        Mockito.verify(testCG).waitForMessage(conn);
        assertEquals(ImmutableList.of(conn), cg.getClients());
    }

    @Test
    public void testMultipleGETsWithNoMessage() throws Exception {
        final ConsumerGroup cg = new ConsumerGroup();
        final ConsumerGroup testCG = Mockito.spy(cg);
        final ConsumerConnection conn = new ConsumerConnection(testCG);
        conn.onWebSocketText("#GET");
        conn.onWebSocketText("#GET");
        conn.onWebSocketText("#GET");
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
        conn.onWebSocketText("#GET");
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
        conn.onWebSocketText("#GET");
        assertEquals(Collections.emptyList(), cg.getClients());
        conn.onWebSocketText("#GET");
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
        conn.onWebSocketText("#GET");
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
        conn.onWebSocketText("#GET");
        Mockito.verify(conn).sendMessage("test_1");
        assertEquals(Collections.emptyList(), cg.getClients());
        conn.onWebSocketText("#GET");
        Mockito.verify(conn).sendMessage("test_2");
        assertEquals(Collections.emptyList(), cg.getClients());
    }
}