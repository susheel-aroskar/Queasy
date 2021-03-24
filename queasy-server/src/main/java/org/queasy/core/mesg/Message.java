package org.queasy.core.mesg;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.Map;

/**
 * @author saroskar
 * Created on: 2021-03-22
 */
public interface Message<T> {

    Splitter.MapSplitter splitter = Splitter.on('&').trimResults().withKeyValueSeparator('=');
    Joiner.MapJoiner joiner = Joiner.on('&').withKeyValueSeparator('=');

    MessageType getType();

    Map<String, String> getHeaders();

    T getBody();

    T toWebSocketMessage();

    default Map<String, String> parseHeaders(final String headerStr) {
        return Strings.isNullOrEmpty(headerStr) ? Collections.emptyMap() : splitter.split(headerStr);
    }

    default String serializeHeaders() {
        return getHeaders() != null ? joiner.join(getHeaders()) : "";
    }

}
