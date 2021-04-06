package org.queasy.core.network;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
public enum Status {

    OK,
    BUSY,
    TIMEOUT,
    MESG_DROP,
    ERROR;

    private final String status;

    Status() {
        status = ":" + name();
    }

    @Override
    public String toString() {
        return status;
    }
}
