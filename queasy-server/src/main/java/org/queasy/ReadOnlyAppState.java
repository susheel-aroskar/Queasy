package org.queasy;

import com.google.common.collect.ImmutableMap;
import org.jdbi.v3.core.Jdbi;

import java.util.HashMap;
import java.util.Map;

/**
 * Single instance of this class is set on ServerConfiguration. Used to share singleton state across all application.
 * For example, JDBI instance. This is supposed to hold immutable, global state.
 *
 * @author saroskar
 * Created on: 2021-03-22
 */
public class ReadOnlyAppState {


    /**
     * Database handle. Singleton
     */
    private final Jdbi jdbi;

    /**
     * Map of field (DB column) name => data type (java.sql.Types)
     */
    private final Map<String, Integer> messageFields;


    public ReadOnlyAppState(final Jdbi jdbi, final Map<String, Integer> messageFields) {
        this.jdbi = jdbi;
        this.messageFields = ImmutableMap.copyOf(messageFields);
    }

    public Jdbi getJdbi() {
        return jdbi;
    }

    public Map<String, Integer> getMessageFields() {
        return messageFields;
    }

}
