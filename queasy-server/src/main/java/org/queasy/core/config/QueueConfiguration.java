package org.queasy.core.config;


import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * @author saroskar
 * Created on: 2021-03-19
 */
public class QueueConfiguration {

    /**
     * Name of the queue. Used to generate database table
     */
    @NotNull
    private String name;

    /**
     * Size of a ring buffer used to queue incoming messages
     */
    @NotNull
    @Min(64)
    private int ringBufferSize=1024;

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
    private Long maxRetention;


    public String getName() {
        return name;
    }

    public int getRingBufferSize() {
        return ringBufferSize;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public Long getMaxRetention() {
        return maxRetention;
    }
}
