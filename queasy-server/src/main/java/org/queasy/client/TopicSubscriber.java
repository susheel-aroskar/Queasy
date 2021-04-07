package org.queasy.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author saroskar
 * Created on: 2021-04-06
 */
public interface TopicSubscriber {

    Logger s_logger = LoggerFactory.getLogger(TopicSubscriber.class);

    default void onDroppedMessages() {
        s_logger.info("Slow subscriber, messages dropped.");
    }

    void onMessage(String message);
    void onError(Throwable t);
    void onClose();
}
