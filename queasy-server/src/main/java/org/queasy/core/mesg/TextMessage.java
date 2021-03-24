package org.queasy.core.mesg;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class TextMessage implements Message<String> {

    private final Map<String, String> headers;
    private final String body;

    private static Splitter headBodySplitter = Splitter.on("\n").limit(2);

    public TextMessage(Map<String, String> headers, String body) {
        this.headers = headers;
        this.body = body;
    }

    public TextMessage(final String payload) {
        final Iterator<String> parsed = headBodySplitter.split(payload).iterator();
        String headerStr = null;
        String bodyStr = null;
        if (parsed.hasNext()) {
            headerStr = parsed.next();
            if (parsed.hasNext()) {
                bodyStr = parsed.next();
            }
        }
        headers = parseHeaders(headerStr);
        body = bodyStr;
    }


    @Override
    public MessageType getType() {
        return MessageType.TEXT;
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public String getBody() {
        return body;
    }

    @Override
    public String toWebSocketMessage() {
        return (body != null) ? serializeHeaders() + '\n' + body : serializeHeaders();
    }

}
