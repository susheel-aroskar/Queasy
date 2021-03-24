package org.queasy.core.mesg;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author saroskar
 * Created on: 2021-03-23
 */
public class TextMessageTest {

    @Test
    public void singleHeaderAndBodyTest() {
        final String s = "name=value\nbody";
        final TextMessage message = new TextMessage(s);
        assertEquals(ImmutableMap.of("name","value"), message.getHeaders());
        assertEquals("body", message.getBody());
        assertEquals(s, message.toWebSocketMessage());
    }

    @Test
    public void multipleHeadersAndBodyTest() {
        final String s = "name1=value1&name2=value2\nbody2";
        final TextMessage message = new TextMessage(s);
        assertEquals(ImmutableMap.of("name1","value1","name2","value2"), message.getHeaders());
        assertEquals("body2", message.getBody());
        assertEquals(s, message.toWebSocketMessage());
    }

    @Test
    public void noHeadersButBodyTest() {
        final String s = "\nbody3";
        final TextMessage message = new TextMessage(s);
        assertEquals(0, message.getHeaders().size());
        assertEquals("body3", message.getBody());
        assertEquals(s, message.toWebSocketMessage());
    }

    @Test
    public void emptyBodySingleHeaderTest() {
        final String s = "name1=value1\n";
        final TextMessage message = new TextMessage(s);
        assertEquals(ImmutableMap.of("name1","value1"), message.getHeaders());
        assertEquals("", message.getBody());
        assertEquals(s, message.toWebSocketMessage());
    }

    @Test
    public void emptyBodyMultipleHeadersTest() {
        final String s = "name1=value1&name2=value2\n";
        final TextMessage message = new TextMessage(s);
        assertEquals(ImmutableMap.of("name1","value1","name2","value2"), message.getHeaders());
        assertEquals("", message.getBody());
        assertEquals(s, message.toWebSocketMessage());
    }

    @Test
    public void noBodyButSingleHeaderTest() {
        final String s = "name1=value1";
        final TextMessage message = new TextMessage(s);
        assertEquals(ImmutableMap.of("name1","value1"), message.getHeaders());
        assertNull(message.getBody());
        assertEquals(s, message.toWebSocketMessage());
    }

    @Test
    public void noBodyButMultipleHeadersTest() {
        final String s = "name1=value1&name2=value2";
        final TextMessage message = new TextMessage(s);
        assertEquals(ImmutableMap.of("name1","value1","name2","value2"), message.getHeaders());
        assertNull(message.getBody());
        assertEquals(s, message.toWebSocketMessage());
    }

}
