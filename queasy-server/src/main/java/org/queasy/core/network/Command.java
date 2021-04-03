package org.queasy.core.network;

import com.google.common.base.Splitter;

import java.util.List;

/**
 * @author saroskar
 * Created on: 2021-03-24
 */
public enum Command {

    GET,
    RECONNECT,
    PING;

    private final String cmd;

    private static final Splitter splitter = Splitter.on(" ").trimResults().omitEmptyStrings();

    Command() {
        cmd = "#" + name() + " ";
    }

    public boolean matches(final String cmd) {
        return (cmd != null && cmd.startsWith(this.cmd));
    }

    public Iterable<String> parse(final String cmd) {
        return splitter.split(cmd);
    }

    @Override
    public String toString() {
        return "#"+name();
    }


}
