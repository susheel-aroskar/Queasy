package org.queasy.core.network;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
public enum Status {

    OK,
    BUSY,
    TIMEOUT,
    ERROR;

    @Override
    public String toString() {
        return ":"+name();
    }
}
