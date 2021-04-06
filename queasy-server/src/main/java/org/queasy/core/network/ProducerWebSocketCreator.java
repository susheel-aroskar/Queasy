package org.queasy.core.network;

import com.google.common.base.Splitter;
import io.dropwizard.util.Strings;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.queasy.core.managed.QueueWriter;

import java.util.List;

import static org.queasy.ServerApplication.PUBLISH_PATH;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ProducerWebSocketCreator extends BaseWebSocketCreator {

    private final QueueWriter queueWriter;

    public static final Splitter pathSplitter = Splitter.on('/').trimResults().omitEmptyStrings();

    public ProducerWebSocketCreator(final String origin, final int maxConnections, final QueueWriter queueWriter) {
        super(origin, maxConnections);
        this.queueWriter = queueWriter;
    }

    @Override
    protected ProducerConnection createConnection(final ServletUpgradeRequest req, final ServletUpgradeResponse resp) {
        final String path = req.getRequestURI() != null ? req.getRequestURI().getPath() : "";
        final List<String> parts = pathSplitter.splitToList(path != null ? path : "");
        if ((parts.size() == 2) && (PUBLISH_PATH.equals(parts.get(0))) && (!Strings.isNullOrEmpty(parts.get(1)))) {
            return new ProducerConnection(queueWriter, parts.get(1));
        } else {
            closeConnection(400, resp);
            return null;
        }
    }

}
