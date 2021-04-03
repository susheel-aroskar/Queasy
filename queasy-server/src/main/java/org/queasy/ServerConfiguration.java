package org.queasy;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.util.Duration;
import org.queasy.core.config.CacheConfiguration;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.QueueConfiguration;
import org.queasy.core.config.WebSocketConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.*;
import java.util.Map;

public class ServerConfiguration extends Configuration {

    /**
     * id of this message queue server. Every message queue server in the queue cluster must have an unique id.
     * It's used in Snowflake unique id generation algorithm.
     */
    @NotNull
    @Min(0)
    @Max(1023)
    private Integer hostId;
    /**
     * Maximum number of producer or writer connections allowed
     */
    @NotNull
    @Min(256)
    private int maxConnections;

    /**
     * NUmber of threads used by the consumer group servicing thread pool
     */
    @NotNull
    @Min(2)
    private int consumerGroupsThreadPoolSize = 8;

    /**
     * Poll interval to check for new messages. It's a very inexpensive operation,
     * so 100s or even 10s of millisecond interval is OK
     */
    private Duration newMessagePollInterval = Duration.milliseconds(250);

    /**
     * Maximum time period to wait for graceful shutdown of application
     */
    private Duration shutdownGracePeriod = Duration.seconds(60);

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
     * System-wide message cache configuration
     * @return
     */
    @Valid
    @NotNull
    private CacheConfiguration cacheConfiguration;


    public int getHostId() {
        return hostId;
    }

    public void setHostId(int hostId) {
        this.hostId = hostId;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getConsumerGroupsThreadPoolSize() {
        return consumerGroupsThreadPoolSize;
    }

    public void setConsumerGroupsThreadPoolSize(int consumerGroupsThreadPoolSize) {
        this.consumerGroupsThreadPoolSize = consumerGroupsThreadPoolSize;
    }

    public Duration getNewMessagePollInterval() {
        return newMessagePollInterval;
    }

    public void setNewMessagePollInterval(Duration newMessagePollInterval) {
        this.newMessagePollInterval = newMessagePollInterval;
    }

    public Duration getShutdownGracePeriod() {
        return shutdownGracePeriod;
    }

    public void setShutdownGracePeriod(Duration shutdownGracePeriod) {
        this.shutdownGracePeriod = shutdownGracePeriod;
    }

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

    public CacheConfiguration getCacheConfiguration() {
        return cacheConfiguration;
    }

    public void setCacheConfiguration(CacheConfiguration cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }
}
