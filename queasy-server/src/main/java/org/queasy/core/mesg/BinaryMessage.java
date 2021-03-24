package org.queasy.core.mesg;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;

import java.util.Map;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class BinaryMessage implements Message<byte[]> {

    private final Map<String, String> headers;
    private final byte[] body;

    public BinaryMessage(Map<String, String> headers, byte[] body) {
        this.headers = headers;
        this.body = body;
    }

    public BinaryMessage(final byte[] payload, final int offset, final int len) {
        String headerStr = null;
        byte[] bodyBuff = null;
        for (int i = 0; i < len; i++) {
            if (payload[offset + i] == '\n') {
                headerStr = new String(payload, offset, i, Charsets.US_ASCII);
                if (i < len) {
                    bodyBuff = new byte[len - (i + 1)];
                    System.arraycopy(payload, offset + i + 1, bodyBuff, 0, bodyBuff.length);
                }
                break;
            }
        }

        if (headerStr == null) {
            headerStr = new String(payload, offset, len, Charsets.UTF_8);
        }

        headers = parseHeaders(headerStr);
        body = bodyBuff;
    }

    @Override
    public MessageType getType() {
        return MessageType.BINARY;
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public byte[] toWebSocketMessage() {
        final String hs = serializeHeaders();
        final int headerLen = Strings.isNullOrEmpty(hs) ? 0 : hs.length();
        final int bodyLen = (body == null) ? 0: body.length;
        final int messageLen = headerLen + 1 + bodyLen;
        final byte[] message = new byte[messageLen];
        if (headerLen != 0) {
            System.arraycopy(hs.getBytes(Charsets.US_ASCII), 0, message, 0, headerLen);
        }
        message[headerLen] = '\n';
        if (bodyLen != 0) {
            System.arraycopy(body, 0, message, headerLen + 1, body.length);
        }
        return message;
    }
}
