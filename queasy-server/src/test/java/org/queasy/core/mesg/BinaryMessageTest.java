package org.queasy.core.mesg;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author saroskar
 * Created on: 2021-03-23
 */
public class BinaryMessageTest {

    private final byte[] body = new byte[] {'b','o','d','y'};
    private final byte[] empty = new byte[0];

    @Test
    public void singleHeaderAndBodyTest() {
        byte[] buff = new byte[]{'n','1','=','v','1','\n','b','o','d','y'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length);
        assertEquals(ImmutableMap.of("n1","v1"), message.getHeaders());
        assertArrayEquals(body, message.getBody());
        assertArrayEquals(buff, message.toWebSocketMessage());
    }

    @Test
    public void singleHeaderAndBodyInMiddleOfBufferTest() {
        byte[] buff = new byte[]{'\n','n','1','=','v','1','\n','b','o','d','y','e','x','t','r','a'};
        final BinaryMessage message = new BinaryMessage(buff, 1, buff.length-6);
        assertEquals(ImmutableMap.of("n1","v1"), message.getHeaders());
        assertArrayEquals(body, message.getBody());
        assertArrayEquals(new byte[]{'n','1','=','v','1','\n','b','o','d','y'}, message.toWebSocketMessage());
    }

    @Test
    public void multipleHeadersAndBodyTest() {
        byte[] buff = new byte[]{'n','1','=','v','1','&','n','2','=','v','2','\n','b','o','d','y'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length);
        assertEquals(ImmutableMap.of("n1","v1","n2","v2"), message.getHeaders());
        assertArrayEquals(body, message.getBody());
        assertArrayEquals(buff, message.toWebSocketMessage());
    }

    @Test
    public void multipleHeadersAndBodyTestAtTheEndOfBuffer() {
        byte[] buff = new byte[]{'\n','=','&','n','1','=','v','1','&','n','2','=','v','2','\n','b','o','d','y'};
        final BinaryMessage message = new BinaryMessage(buff, 3, buff.length-3);
        assertEquals(ImmutableMap.of("n1","v1","n2","v2"), message.getHeaders());
        assertArrayEquals(body, message.getBody());
        assertArrayEquals(new byte[]{'n','1','=','v','1','&','n','2','=','v','2','\n','b','o','d','y'},
                message.toWebSocketMessage());
    }

    @Test
    public void noHeadersButBodyTest() {
        byte[] buff = new byte[]{'\n','b','o','d','y'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length);
        assertEquals(0, message.getHeaders().size());
        assertArrayEquals(body, message.getBody());
        assertArrayEquals(buff, message.toWebSocketMessage());
    }

    @Test
    public void noHeadersButBodyTestAtTheStartOfBuffer() {
        byte[] buff = new byte[]{'\n','b','o','d','y','e','x'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length-2);
        assertEquals(0, message.getHeaders().size());
        assertArrayEquals(body, message.getBody());
        assertArrayEquals(new byte[]{'\n','b','o','d','y'}, message.toWebSocketMessage());
    }

    @Test
    public void emptyBodySingleHeaderTest() {
        byte[] buff = new byte[]{'n','1','=','v','1','\n'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length);
        assertEquals(ImmutableMap.of("n1","v1"), message.getHeaders());
        assertArrayEquals(empty, message.getBody());
        assertArrayEquals(buff, message.toWebSocketMessage());
    }

    @Test
    public void emptyBodySingleHeaderTestInTheMiddleOfTheBuffer() {
        byte[] buff = new byte[]{'\n','n','1','=','v','1','\n','b','o','d','y'};
        final BinaryMessage message = new BinaryMessage(buff, 1, buff.length-5);
        assertEquals(ImmutableMap.of("n1","v1"), message.getHeaders());
        assertArrayEquals(empty, message.getBody());
        assertArrayEquals(new byte[]{'n','1','=','v','1','\n'}, message.toWebSocketMessage());
    }

    @Test
    public void emptyBodyMultipleHeadersTest() {
        byte[] buff = new byte[]{'n','1','=','v','1','&','n','2','=','v','2','\n'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length);
        assertEquals(ImmutableMap.of("n1","v1","n2","v2"), message.getHeaders());
        assertArrayEquals(empty, message.getBody());
        assertArrayEquals(buff, message.toWebSocketMessage());
    }

    @Test
    public void emptyBodyMultipleHeadersAtTheStartOfBufferTest() {
        byte[] buff = new byte[]{'n','1','=','v','1','&','n','2','=','v','2','\n','b','o','d','y'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length-4);
        assertEquals(ImmutableMap.of("n1","v1","n2","v2"), message.getHeaders());
        assertArrayEquals(empty, message.getBody());
        assertArrayEquals(new byte[]{'n','1','=','v','1','&','n','2','=','v','2','\n'}, message.toWebSocketMessage());
    }

    @Test
    public void noBodyButSingleHeaderTest() {
        byte[] buff = new byte[]{'n','1','=','v','1'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length);
        assertEquals(ImmutableMap.of("n1","v1"), message.getHeaders());
        assertNull(message.getBody());
        assertArrayEquals(new byte[]{'n','1','=','v','1','\n'}, message.toWebSocketMessage());
    }

    @Test
    public void noBodyButSingleHeaderTestInTheMiddleOfBuffer() {
        byte[] buff = new byte[]{'\n','n','1','=','v','1','\n','b','o','d','y'};
        final BinaryMessage message = new BinaryMessage(buff, 1, buff.length-6);
        assertEquals(ImmutableMap.of("n1","v1"), message.getHeaders());
        assertNull(message.getBody());
        assertArrayEquals(new byte[]{'n','1','=','v','1','\n'}, message.toWebSocketMessage());
    }

    @Test
    public void noBodyButMultipleHeadersTest() {
        byte[] buff = new byte[]{'n','1','=','v','1','&','n','2','=','v','2'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length);
        assertEquals(ImmutableMap.of("n1","v1","n2","v2"), message.getHeaders());
        assertNull(message.getBody());
        assertArrayEquals(new byte[]{'n','1','=','v','1','&','n','2','=','v','2','\n'}, message.toWebSocketMessage());
    }

    @Test
    public void noBodyButMultipleHeadersTestAtTheEndOfBuffer() {
        byte[] buff = new byte[]{'n','1','=','v','1','&','n','2','=','v','2','\n','b','o','d','y'};
        final BinaryMessage message = new BinaryMessage(buff, 0, buff.length-5);
        assertEquals(ImmutableMap.of("n1","v1","n2","v2"), message.getHeaders());
        assertNull(message.getBody());
        assertArrayEquals(new byte[]{'n','1','=','v','1','&','n','2','=','v','2','\n'}, message.toWebSocketMessage());
    }
}
