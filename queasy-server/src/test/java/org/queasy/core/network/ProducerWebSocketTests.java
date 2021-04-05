package org.queasy.core.network;

import io.dropwizard.util.Duration;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.queasy.core.config.WriterConfiguration;
import org.queasy.core.managed.QueueWriter;

import java.io.IOException;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
public class ProducerWebSocketTests {

    @BeforeEach
    public void setUp() {
        BaseWebSocketConnection.resetConnectionCount();
    }

    @Test
    public void testHappyPath() throws Exception {
        final ServletUpgradeRequest request = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(request.getOrigin()).thenReturn("foo.com");
        Mockito.when(request.getRequestURI()).thenReturn(new URI("/nq/test"));
        final ServletUpgradeResponse response = Mockito.mock(ServletUpgradeResponse.class);
        ProducerWebSocketCreator creator = new ProducerWebSocketCreator("foo.com", 4, null);
        final ProducerConnection conn = (ProducerConnection) creator.createWebSocket(request, response);
        assertEquals("test", conn.getQName());
    }

    @Test
    public void testWrongOrigin() throws Exception {
        final ServletUpgradeRequest request = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(request.getOrigin()).thenReturn("foo.com");
        Mockito.when(request.getRequestURI()).thenReturn(new URI("/nq/test"));
        final ServletUpgradeResponse response = new ServletUpgradeResponse(null);
        final ServletUpgradeResponse response1 = Mockito.spy(response);
        ProducerWebSocketCreator creator = new ProducerWebSocketCreator("foo1.com", 4, null);
        final WebSocketListener conn = creator.createWebSocket(request, response1);
        assertNull(conn);
        Mockito.verify(response1).setStatusCode(403);
    }

    @Test
    public void testWrongURI1() throws Exception {
        final ServletUpgradeRequest request = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(request.getOrigin()).thenReturn("foo.com");
        Mockito.when(request.getRequestURI()).thenReturn(new URI("/dq/test"));
        final ServletUpgradeResponse response = new ServletUpgradeResponse(null);
        final ServletUpgradeResponse response1 = Mockito.spy(response);
        ProducerWebSocketCreator creator = new ProducerWebSocketCreator("foo.com", 4, null);
        final WebSocketListener conn = creator.createWebSocket(request, response1);
        assertNull(conn);
        Mockito.verify(response1).setStatusCode(400);
    }

    @Test
    public void testWrongURI2() throws Exception {
        final ServletUpgradeRequest request = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(request.getOrigin()).thenReturn("foo.com");
        Mockito.when(request.getRequestURI()).thenReturn(new URI("/nq/test/too/long"));
        final ServletUpgradeResponse response = new ServletUpgradeResponse(null);
        final ServletUpgradeResponse response1 = Mockito.spy(response);
        ProducerWebSocketCreator creator = new ProducerWebSocketCreator("foo.com", 4, null);
        final WebSocketListener conn = creator.createWebSocket(request, response1);
        assertNull(conn);
        Mockito.verify(response1).setStatusCode(400);
    }

    @Test
    public void testMaxConnectionsError() throws Exception {
        final ServletUpgradeRequest request = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(request.getOrigin()).thenReturn("foo.com");
        Mockito.when(request.getRequestURI()).thenReturn(new URI("/nq/test"));
        final ServletUpgradeResponse response = new ServletUpgradeResponse(null);
        final ServletUpgradeResponse response1 = Mockito.spy(response);
        ProducerWebSocketCreator creator = new ProducerWebSocketCreator("foo.com", 2, null);

        final Session session = Mockito.mock(Session.class);
        final WebSocketListener conn1 = creator.createWebSocket(request, response1);
        assertNotNull(conn1);
        conn1.onWebSocketConnect(session);

        final WebSocketListener conn2 = creator.createWebSocket(request, response1);
        assertNotNull(conn2);
        conn2.onWebSocketConnect(session);

        final WebSocketListener conn3 = creator.createWebSocket(request, response1);
        assertNull(conn3);
        Mockito.verify(response1).setStatusCode(503);
    }

    @Test
    public void testInvalidConnectionsDoNotCountTowardsMaxConnections() throws Exception {
        final ServletUpgradeRequest request = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(request.getOrigin()).thenReturn("foo.com");
        Mockito.when(request.getRequestURI()).thenReturn(new URI("/nq/test"));

        final ServletUpgradeRequest invalidRequest1 = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(invalidRequest1.getOrigin()).thenReturn("foo1.com");
        Mockito.when(invalidRequest1.getRequestURI()).thenReturn(new URI("/nq/test"));

        final ServletUpgradeRequest invalidRequest2 = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(invalidRequest2.getOrigin()).thenReturn("foo.com");
        Mockito.when(invalidRequest2.getRequestURI()).thenReturn(new URI("/dq/test"));

        final ServletUpgradeResponse response = new ServletUpgradeResponse(null);
        final ServletUpgradeResponse response1 = Mockito.spy(response);

        ProducerWebSocketCreator creator = new ProducerWebSocketCreator("foo.com", 2, null);
        final Session session = Mockito.mock(Session.class);

        final WebSocketListener conn1 = creator.createWebSocket(request, response1);
        assertNotNull(conn1);
        conn1.onWebSocketConnect(session);

        final WebSocketListener conn2 = creator.createWebSocket(invalidRequest1, response1);
        assertNull(conn2);
        Mockito.verify(response1).setStatusCode(403);

        final WebSocketListener conn3 = creator.createWebSocket(invalidRequest2, response1);
        assertNull(conn3);
        Mockito.verify(response1).setStatusCode(400);

        final WebSocketListener conn4 = creator.createWebSocket(request, response1);
        assertNotNull(conn4);
        conn4.onWebSocketConnect(session);

        final WebSocketListener conn5 = creator.createWebSocket(request, response1);
        assertNull(conn5);
        Mockito.verify(response1).setStatusCode(503);
    }

    @Test
    public void testClosedAndInvalidConnectionsDoNotCountTowardsMaxConnections() throws Exception {
        final ServletUpgradeRequest request = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(request.getOrigin()).thenReturn("foo.com");
        Mockito.when(request.getRequestURI()).thenReturn(new URI("/nq/test"));

        final ServletUpgradeRequest invalidRequest1 = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(invalidRequest1.getOrigin()).thenReturn("foo1.com");
        Mockito.when(invalidRequest1.getRequestURI()).thenReturn(new URI("/nq/test"));

        final ServletUpgradeRequest invalidRequest2 = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(invalidRequest2.getOrigin()).thenReturn("foo.com");
        Mockito.when(invalidRequest2.getRequestURI()).thenReturn(new URI("/dq/test"));

        final ServletUpgradeResponse response = new ServletUpgradeResponse(null);
        final ServletUpgradeResponse response1 = Mockito.spy(response);

        ProducerWebSocketCreator creator = new ProducerWebSocketCreator("foo.com", 2, null);
        final Session session = Mockito.mock(Session.class);

        final WebSocketListener conn1 = creator.createWebSocket(request, response1);
        assertNotNull(conn1);
        conn1.onWebSocketConnect(session);

        final WebSocketListener conn2 = creator.createWebSocket(invalidRequest1, response1);
        assertNull(conn2);
        Mockito.verify(response1).setStatusCode(403);

        final WebSocketListener conn3 = creator.createWebSocket(invalidRequest2, response1);
        assertNull(conn3);
        Mockito.verify(response1).setStatusCode(400);

        final WebSocketListener conn4 = creator.createWebSocket(request, response1);
        assertNotNull(conn4);
        conn4.onWebSocketConnect(session);

        final WebSocketListener conn5 = creator.createWebSocket(request, response1);
        assertNull(conn5);
        Mockito.verify(response1).setStatusCode(503);

        conn1.onWebSocketClose(200, "");
        final WebSocketListener conn6 = creator.createWebSocket(request, response1);
        assertNotNull(conn6);
        conn6.onWebSocketConnect(session);

        final WebSocketListener conn7 = creator.createWebSocket(request, response1);
        assertNull(conn7);

        conn6.onWebSocketClose(200, "");
        final WebSocketListener conn8 = creator.createWebSocket(request, response1);
        assertNotNull(conn8);
        conn8.onWebSocketConnect(session);
    }

    @Test
    public void testSomeClosesAndSomeErrors() throws Exception {
        final ServletUpgradeRequest request = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(request.getOrigin()).thenReturn("foo.com");
        Mockito.when(request.getRequestURI()).thenReturn(new URI("/nq/test"));

        final ServletUpgradeRequest invalidRequest1 = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(invalidRequest1.getOrigin()).thenReturn("foo1.com");
        Mockito.when(invalidRequest1.getRequestURI()).thenReturn(new URI("/nq/test"));

        final ServletUpgradeRequest invalidRequest2 = Mockito.mock(ServletUpgradeRequest.class);
        Mockito.when(invalidRequest2.getOrigin()).thenReturn("foo.com");
        Mockito.when(invalidRequest2.getRequestURI()).thenReturn(new URI("/dq/test"));

        final ServletUpgradeResponse response = new ServletUpgradeResponse(null);
        final ServletUpgradeResponse response1 = Mockito.spy(response);

        ProducerWebSocketCreator creator = new ProducerWebSocketCreator("foo.com", 2, null);
        final Session session = Mockito.mock(Session.class);

        final WebSocketListener conn1 = creator.createWebSocket(request, response1);
        assertNotNull(conn1);
        conn1.onWebSocketConnect(session);

        final WebSocketListener conn2 = creator.createWebSocket(request, response1);
        assertNotNull(conn2);
        conn2.onWebSocketConnect(session);
        conn2.onWebSocketClose(200, "s");

        final WebSocketListener conn3 = creator.createWebSocket(request, response1);
        assertNotNull(conn3);
        conn3.onWebSocketConnect(session);
        conn3.onWebSocketError(new RuntimeException());

        //make sure close + error != double decrement
        final WebSocketListener conn4 = creator.createWebSocket(request, response1);
        assertNotNull(conn4);
        conn4.onWebSocketConnect(session);
        conn4.onWebSocketClose(200, "s");
        conn4.onWebSocketError(new RuntimeException());

        final WebSocketListener conn5 = creator.createWebSocket(request, response1);
        assertNotNull(conn5);
        conn5.onWebSocketConnect(session);

        final WebSocketListener conn6 = creator.createWebSocket(request, response1);
        assertNull(conn6);
    }

    @Test
    public void testWriteMessageHappyPath() throws IOException  {
        final Session session = Mockito.mock(Session.class);
        final RemoteEndpoint remote = Mockito.mock(RemoteEndpoint.class);
        Mockito.doReturn(remote).when(session).getRemote();

        final WriterConfiguration wc = new WriterConfiguration();
        wc.setRingBufferSize(2);
        final QueueWriter qw = new QueueWriter(wc,null);
        final ProducerConnection conn = new ProducerConnection(qw, "test");
        conn.onWebSocketConnect(session);

        conn.onWebSocketText("test");
        Mockito.verify(remote).sendString(":OK", conn);
    }

    @Test
    public void testTimeOutOnFull() throws IOException  {
        final Session session = Mockito.mock(Session.class);
        final RemoteEndpoint remote = Mockito.mock(RemoteEndpoint.class);
        Mockito.doReturn(remote).when(session).getRemote();

        final WriterConfiguration wc = new WriterConfiguration();
        wc.setRingBufferSize(1);
        wc.setWriteTimeout(Duration.milliseconds(10));
        final QueueWriter qw = new QueueWriter(wc,null);
        final ProducerConnection conn = new ProducerConnection(qw, "test");
        conn.onWebSocketConnect(session);

        conn.onWebSocketText("test1");
        Mockito.verify(remote).sendString(":OK", conn);
        conn.onWebSocketText("test2");
        Mockito.verify(remote).sendString(":TIMEOUT", conn);
    }

    @Test
    public void testBusy() throws Exception  {
        final Session session = Mockito.mock(Session.class);
        final RemoteEndpoint remote = Mockito.mock(RemoteEndpoint.class);
        Mockito.doReturn(remote).when(session).getRemote();

        final WriterConfiguration wc = new WriterConfiguration();
        wc.setRingBufferSize(1);
        wc.setWriteTimeout(Duration.seconds(1));
        final QueueWriter qw = new QueueWriter(wc,null);
        final ProducerConnection conn = new ProducerConnection(qw, "test");
        conn.onWebSocketConnect(session);

        conn.onWebSocketText("test1");
        Mockito.verify(remote).sendString(":OK", conn);

        final Thread t = new Thread(() -> {
            try {
                conn.onWebSocketText("test2");
                Mockito.verify(remote).sendString(":TIMEOUT", conn);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        t.start();
        Thread.sleep(100);

        conn.onWebSocketText("test3");
        Mockito.verify(remote).sendString(":BUSY", conn);

        t.join();
        qw.drainQueue();
        conn.onWebSocketText("test4");
        Mockito.verify(remote, Mockito.times(2)).sendString(":OK", conn);

        conn.onWebSocketText("test5");
        Mockito.verify(remote, Mockito.times(2)).sendString(":TIMEOUT", conn);
    }

}
