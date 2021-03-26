package org.queasy.core.network;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
public enum Command {

    GET,
    RECONNECT,
    PING;

    @Override
    public String toString() {
        return "#"+name();
    }
}
