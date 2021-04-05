package org.queasy.core.managed;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author saroskar
 * Created on: 2021-04-04
 */
public class JsonInsertTest {

    private Map<String, Map> model;
    private String json;
    private ObjectMapper mapper;

    @BeforeEach
    public void setup() throws Exception {
        mapper = new ObjectMapper();
        model =ImmutableMap.of(
                "headers", ImmutableMap.of("Content-Length", 1024, "Content-Type", "text/json", "error", true),
                "payload", ImmutableMap.of("body", "test_body", "status", 200)
        );
        json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(model);
    }

    @Test
    public void testAddEnvelope() throws Exception {
//        System.out.println("Json: " + json);
        String envelope = String.format("{\"id\": %s, \"retry\": %s, \"body\": %s}", 512, true, json);
//        System.out.println("envelope: " + envelope);
        Map<String, Map> parsed = mapper.readValue(envelope, HashMap.class);
        Assertions.assertEquals(512, parsed.get("id"));
        Assertions.assertEquals(true, parsed.get("retry"));
        Assertions.assertEquals(model, parsed.get("body"));
    }
}
