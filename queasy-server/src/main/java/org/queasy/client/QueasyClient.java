package org.queasy.client;

import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.net.URI;
import java.util.concurrent.Future;

/**
 * @author saroskar
 * Created on: 2021-04-06
 */
public final class QueasyClient implements Managed {

    public static final String PUBLISH_PATH = "publish";
    public static final String DEQUEUE_PATH = "dequeue";
    public static final String SUBSCRIBE_PATH = "subscribe";
    private final WebSocketClient client;
    private final String origin;


    public QueasyClient(final String origin) {
        client = new WebSocketClient();
        this.origin = origin;
    }

    public QueasyClient() {
        this(null);
    }

    public void start() throws Exception {
        client.start();
    }

    @Override
    public void stop() throws Exception {
        client.stop();
    }

    private Future<Session> connect(final String baseURI, final String pathPrefix, final String name,
                                    final WebSocketAdapter ws) throws Exception {
        Preconditions.checkNotNull(baseURI, "Base URI must be provided");
        Preconditions.checkNotNull(pathPrefix, "Path prefix must be provided");
        Preconditions.checkNotNull(name, "Name must be provided");

        final URI uri = URI.create(baseURI + pathPrefix + "/" + name);
        final ClientUpgradeRequest request = new ClientUpgradeRequest();
        request.setRequestURI(uri);
        request.setLocalEndpoint(ws);
        if (origin != null) {
            request.setHeader("Origin", origin);
        }

        return client.connect(ws, uri, request);
    }

    public QueueProducer connectProducer(final String baseURI,
                                         final String queueName) throws Exception {
        final QueueProducer queueProducer = new QueueProducer();
        final Future<Session> f = connect(baseURI, PUBLISH_PATH, queueName, queueProducer.getQueueConnection());
        f.get();
        return queueProducer;
    }

    public ConsumerGroupMember connectToConsumerGroup(final String baseURI,
                                                      final String consumerGroupName) throws Exception {
        final ConsumerGroupMember cgm = new ConsumerGroupMember();
        final Future<Session> f = connect(baseURI, DEQUEUE_PATH, consumerGroupName, cgm.getQueueConnection());
        f.get();
        return cgm;
    }


    public void subscribeToTopic(final String baseURI, final String topicName,
                                 final TopicSubscriber topicSubscriber) throws Exception {
        final Future<Session> f = connect(baseURI, SUBSCRIBE_PATH, topicName, new TopicSubscription(topicSubscriber));
        f.get();
    }

}
