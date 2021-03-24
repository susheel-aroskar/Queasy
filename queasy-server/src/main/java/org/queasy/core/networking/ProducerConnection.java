package org.queasy.core.networking;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.queasy.core.managed.Producer;
import org.queasy.core.mesg.BinaryMessage;
import org.queasy.core.mesg.TextMessage;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ProducerConnection extends WebSocketAdapter {

    private final Producer producer;

    public ProducerConnection(Producer producer) {
        this.producer = producer;
    }

    @Override
    public void onWebSocketText(String message) {
        try {
            producer.enqueue(new TextMessage(message));
        }
        catch (InterruptedException ex) {
            getSession().close();
        }
    }

    @Override
    public void onWebSocketBinary(byte[] payload, int offset, int len) {
        try {
            producer.enqueue(new BinaryMessage(payload, offset, len));
        }
        catch (InterruptedException ex) {
            getSession().close();
        }
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        super.onWebSocketClose(statusCode, reason);
    }

    @Override
    public void onWebSocketConnect(Session sess) {
        super.onWebSocketConnect(sess);
    }

    @Override
    public void onWebSocketError(Throwable cause) {
        super.onWebSocketError(cause);
    }

}
