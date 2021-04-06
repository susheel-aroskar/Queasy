package org.queasy;

import io.dropwizard.Configuration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.util.Duration;
import org.queasy.core.config.CacheConfiguration;
import org.queasy.core.config.ConsumerGroupConfiguration;
import org.queasy.core.config.TopicConfiguration;
import org.queasy.core.config.WriterConfiguration;
import org.queasy.core.config.WebSocketConfiguration;

import javax.validation.Valid;
import javax.validation.constraints.*;
import java.util.Map;

public class ServerConfiguration extends Configuration {

    /**
     * id of this message writerConfiguration server. Every message writerConfiguration server in the writerConfiguration cluster must have an unique id.
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
    private int messageDispatcherThreadPoolSize = 8;

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
     * WriterConfiguration properties
     */
    @Valid
    @NotNull
    private WriterConfiguration writerConfiguration;

    /**
     * Consumer groups config
     */
    @Valid
    private Map<String, ConsumerGroupConfiguration> consumerGroups;

    /**
     * Topics config
     */
    @Valid
    private Map<String, TopicConfiguration> topics;

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

    public int getMessageDispatcherThreadPoolSize() {
        return messageDispatcherThreadPoolSize;
    }

    public void setMessageDispatcherThreadPoolSize(int messageDispatcherThreadPoolSize) {
        this.messageDispatcherThreadPoolSize = messageDispatcherThreadPoolSize;
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

    public WriterConfiguration getWriterConfiguration() {
        return writerConfiguration;
    }


    public Map<String, ConsumerGroupConfiguration> getConsumerGroups() {
        return consumerGroups;
    }

    public void setConsumerGroups(Map<String, ConsumerGroupConfiguration> consumerGroups) {
        this.consumerGroups = consumerGroups;
    }

    public Map<String, TopicConfiguration> getTopics() {
        return topics;
    }

    public void setTopics(Map<String, TopicConfiguration> topics) {
        this.topics = topics;
    }

    public CacheConfiguration getCacheConfiguration() {
        return cacheConfiguration;
    }

    public void setCacheConfiguration(CacheConfiguration cacheConfiguration) {
        this.cacheConfiguration = cacheConfiguration;
    }
}
