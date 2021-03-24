package org.queasy.core.config;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public class WebSocketConfiguration {

    private int maxTextMessageSize = 65536;

    private int maxTextMessageBufferSize = 32768;

    private int maxBinaryMessageSize = 65536;

    private int maxBinaryMessageBufferSize = 32768;

    private long asyncWriteTimeout = 60000L;

    private long idleTimeout = 300000L;

    private int inputBufferSize = 4096;

    private String origin;


    public int getMaxTextMessageSize() {
        return maxTextMessageSize;
    }

    public int getMaxTextMessageBufferSize() {
        return maxTextMessageBufferSize;
    }

    public int getMaxBinaryMessageSize() {
        return maxBinaryMessageSize;
    }

    public int getMaxBinaryMessageBufferSize() {
        return maxBinaryMessageBufferSize;
    }

    public long getAsyncWriteTimeout() {
        return asyncWriteTimeout;
    }

    public long getIdleTimeout() {
        return idleTimeout;
    }

    public int getInputBufferSize() {
        return inputBufferSize;
    }

    public String getOrigin() {
        return origin;
    }
}
