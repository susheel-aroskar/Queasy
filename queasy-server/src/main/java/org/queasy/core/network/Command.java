package org.queasy.core.network;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

import java.util.Iterator;
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
        cmd = "#" + name();
    }

    public boolean matches(final String cmd) {
        return (cmd != null && cmd.startsWith(this.cmd));
    }

    public Iterator<String> parse(final String mesg) {
        Iterator<String> iter = splitter.split(mesg).iterator();
        Preconditions.checkArgument(iter.hasNext(), mesg);
        Preconditions.checkArgument(cmd.equals(iter.next()), mesg); // Advance past command name part
        return iter;
    }

    public Long nextArgAsLong(Iterator<String> parsedCmd) {
        return parsedCmd.hasNext() ? Long.valueOf(parsedCmd.next()) : null;
    }

    public Integer nextArgAsInt(Iterator<String> parsedCmd) {
        return parsedCmd.hasNext() ? Integer.valueOf(parsedCmd.next()) : null;
    }

    public Boolean nextArgAsBoolean(Iterator<String> parsedCmd) {
        return parsedCmd.hasNext() ? Boolean.valueOf(parsedCmd.next()) : null;
    }

    public String nextArgAsString(Iterator<String> parsedCmd) {
        return parsedCmd.hasNext() ? parsedCmd.next() : null;
    }

    @Override
    public String toString() {
        return cmd;
    }


}
