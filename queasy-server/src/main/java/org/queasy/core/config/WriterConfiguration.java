package org.queasy.core.config;


import io.dropwizard.util.Duration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * @author saroskar
 * Created on: 2021-03-19
 */
public class WriterConfiguration {

    /**
     * Database table name where queue messages are persisted
     */
    @NotNull
    private String tableName;

    /**
     * Size of a ring buffer used to hold incoming messages
     */
    @NotNull
    @Min(64)
    private int ringBufferSize=1024;

    /**
     * Insert batch size
     */
    @NotNull
    @Max(256)
    private int insertBatchSize=32;

    /**
     * Maximum number of messages to be retained in the DB for this queue.
     */
    @NotNull
    @Min(1024)
    private long maxSize;

    /**
     * Maximum duration for which past messages should be retained in the DB for this queue.
     * Messages older than this limit will be deleted periodically.
     * Do not specify if no limit should be imposed
     */
    private Duration maxRetention;

    /**
     * Max time a write message operation will wait on the internal ring buffer, for space to become available in it,
     * before returning TIMEOUT to the client
     */
    private Duration writeTimeout = Duration.seconds(1);



    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public int getRingBufferSize() {
        return ringBufferSize;
    }

    public void setRingBufferSize(int ringBufferSize) {
        this.ringBufferSize = ringBufferSize;
    }

    public int getInsertBatchSize() {
        return insertBatchSize;
    }

    public void setInsertBatchSize(int insertBatchSize) {
        this.insertBatchSize = insertBatchSize;
    }

    public Duration getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(Duration writeTimeout) {
        this.writeTimeout = writeTimeout;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public Duration getMaxRetention() {
        return maxRetention;
    }

    public void setMaxRetention(Duration maxRetention) {
        this.maxRetention = maxRetention;
    }
}
