package org.queasy.core.config;

import io.dropwizard.util.Duration;

import javax.validation.constraints.NotNull;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ConsumerGroupConfiguration {

    /**
     * Query, in terms of the user defined message metadata fields (qname etc.), if any
     * use standard SQL where clause syntax except the keyword "where"
     */
    @NotNull
    private String query;

    /**
     * How many messages to select in a single batch or poll
     */
    @NotNull
    private int fetchBatchSize = 128;

    /**
     * Queue client timeout. If no message is received in the time window client receives a :TIMEOUT status
     */
    @NotNull
    private Duration timeOut = Duration.minutes(30);

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getFetchBatchSize() {
        return fetchBatchSize;
    }

    public void setFetchBatchSize(int fetchBatchSize) {
        this.fetchBatchSize = fetchBatchSize;
    }

    public Duration getTimeOut() {
        return timeOut;
    }

    public void setTimeOut(Duration timeOut) {
        this.timeOut = timeOut;
    }

}
