package org.queasy;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.QueueConfiguration;
import org.queasy.core.config.WebSocketConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.*;
import java.util.Map;

public class ServerConfiguration extends Configuration {

    /**
     * Database settings
     */
    @Valid
    @NotNull
    private DataSourceFactory database = new DataSourceFactory();

    /**
     * Websocket configuration
     */
    @Valid
    @NotNull
    private WebSocketConfiguration webSocketConfiguration = new WebSocketConfiguration();

    /**
     * QueueConfiguration properties
     */
    @Valid
    @NotNull
    private QueueConfiguration queue;

    /**
     * Consumer groups config
     */
    @Valid
    @NotNull
    private Map<String, ConsumerGroupConfiguration> consumerGroups;

    /**
     * Application wide immutable, read only, singleton state
     */
    private ReadOnlyAppState readOnlyAppState;


    public DataSourceFactory getDatabase() {
        return database;
    }

    public WebSocketConfiguration getWebSocketConfiguration() {
        return webSocketConfiguration;
    }

    public QueueConfiguration getQueue() {
        return queue;
    }


    public Map<String, ConsumerGroupConfiguration> getConsumerGroups() {
        return consumerGroups;
    }

    public void setConsumerGroups(Map<String, ConsumerGroupConfiguration> consumerGroups) {
        this.consumerGroups = consumerGroups;
    }

    public ReadOnlyAppState getReadOnlyAppState() {
        return readOnlyAppState;
    }

    /**
     * ReadOnlyAppState is supposed to be immutable once server start up is complete. That's why this method is package
     * private to ensure it can only be called by ServerApplication - which shares the package space with this class -
     * during application startup
     *
     * @param readOnlyAppState
     */
    void setReadOnlyAppState(final ReadOnlyAppState readOnlyAppState) {
        this.readOnlyAppState = readOnlyAppState;
    }
}
