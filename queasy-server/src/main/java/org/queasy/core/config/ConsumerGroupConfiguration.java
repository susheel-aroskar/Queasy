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
     * Query, in terms of the user defined message metadata fields (qname etc.), if any
     * use standard SQL where clause syntax except the keyword "where"
     */
    @NotNull
    private String query;

    /**
     * How many messages to select in a single batch or poll
     */
    @NotNull
    private int selectBatchSize = 128;


    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getSelectBatchSize() {
        return selectBatchSize;
    }

    public void setSelectBatchSize(int selectBatchSize) {
        this.selectBatchSize = selectBatchSize;
    }
}
