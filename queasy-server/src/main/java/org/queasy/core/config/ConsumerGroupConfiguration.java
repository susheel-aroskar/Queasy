package org.queasy.core.config;

import io.dropwizard.util.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ConsumerGroupConfiguration {

    /**
     * Name of the queue
     */
    @NotNull
    private String queueName;

    /**
     * Number of threads dispatching messages to consumers of this group
     */
    @NotNull
    @Min(1)
    private int threadPoolSize = 1;

    /**
     * How many messages to select in a single batch or poll
     */
    @NotNull
    @Min(32)
    private int selectBatchSize = 128;

    /**
     * Query, in terms of the user defined message metadata fields (type, event, priority) etc., if any
     * use standard SQL where clause syntax except the keyword "where"
     */
    private String query;


    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public int getSelectBatchSize() {
        return selectBatchSize;
    }

    public void setSelectBatchSize(int selectBatchSize) {
        this.selectBatchSize = selectBatchSize;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
