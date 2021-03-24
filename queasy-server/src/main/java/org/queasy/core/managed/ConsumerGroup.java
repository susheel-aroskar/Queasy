package org.queasy.core.managed;

import io.dropwizard.lifecycle.Managed;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.QueueConfiguration;
import org.queasy.db.QueueSchema;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ConsumerGroup implements Managed {

    private final String name;
    private final ConsumerGroupConfiguration consumerGroupConfiguration;
    private final QueueSchema queueSchema;
    private final Producer producer;

    public ConsumerGroup(String name, ConsumerGroupConfiguration consumerGroupConfiguration,
                         QueueSchema queueSchema, Producer producer) {
        this.name = name;
        this.consumerGroupConfiguration = consumerGroupConfiguration;
        this.queueSchema = queueSchema;
        this.producer = producer;
    }

    public String getName() {
        return name;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() throws Exception {

    }
}
