package org.queasy.core.util;

import java.security.SecureRandom;
import java.time.Instant;

/**
 * Distributed Sequence Generator.
 * Inspired by Twitter snowflake: https://github.com/twitter/snowflake/tree/snowflake-2010
 *
 * This class should be used as a Singleton.
 * Make sure that you create and reuse a Single instance of Snowflake per node in your distributed system cluster.
 */
public class Snowflake {

    private final long nodeId;

    private volatile long lastTimestamp = -1L;
    private volatile long sequence = 0L;

    private static final int UNUSED_BITS = 1; // Sign bit, Unused (always set to 0)
    private static final int EPOCH_BITS = 41;
    private static final int NODE_ID_BITS = 10;
    private static final int SEQUENCE_BITS = 12;
    private static final long MAX_NODE_ID = (1L << NODE_ID_BITS) - 1;
    private static final long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1;
    private static final long MASK_NODE_ID = ((1L << NODE_ID_BITS) - 1) << SEQUENCE_BITS;
    private static final long MASK_SEQUENCE = (1L << SEQUENCE_BITS) - 1;
    // Custom Epoch (April 1, 2021 Midnight UTC = 2015-01-01T00:00:00Z)
    private static final long DEFAULT_CUSTOM_EPOCH = Instant.parse("2021-04-01T00:00:00Z").toEpochMilli();


    // Create Snowflake with a nodeId and custom epoch
    public Snowflake(long nodeId) {
        if(nodeId < 0 || nodeId > MAX_NODE_ID) {
            throw new IllegalArgumentException(String.format("NodeId must be between %d and %d", 0, MAX_NODE_ID));
        }
        this.nodeId = nodeId;
    }

    public synchronized long nextId() {
        long currentTimestamp = timestamp();

        while (currentTimestamp < lastTimestamp) {
            //NTP reset the clock backwards?
            Thread.yield();
            currentTimestamp = timestamp();
        }

        if (currentTimestamp == lastTimestamp) {
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if(sequence == 0) {
                // Sequence Exhausted, wait till next millisecond.
                while (currentTimestamp == lastTimestamp) {
                    Thread.yield();
                    currentTimestamp = timestamp();
                }
            }
        } else {
            // reset sequence to start with zero for the next millisecond
            sequence = 0;
        }

        lastTimestamp = currentTimestamp;

        long id = currentTimestamp << (NODE_ID_BITS + SEQUENCE_BITS)
                | (nodeId << SEQUENCE_BITS)
                | sequence;

        return id;
    }


    // Get current timestamp in milliseconds, adjust for the custom epoch.
    private long timestamp() {
        return Instant.now().toEpochMilli() - DEFAULT_CUSTOM_EPOCH;
    }

    public long[] parse(long id) {
        long timestamp = (id >> (NODE_ID_BITS + SEQUENCE_BITS)) + DEFAULT_CUSTOM_EPOCH;
        long nodeId = (id & MASK_NODE_ID) >> SEQUENCE_BITS;
        long sequence = id & MASK_SEQUENCE;

        return new long[]{timestamp, nodeId, sequence};
    }

}