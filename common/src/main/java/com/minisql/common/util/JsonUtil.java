package com.minisql.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Thin wrapper around Jackson for JSON serialization / deserialization.
 */
public final class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtil() {}

    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON serialization failed", e);
        }
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON deserialization failed for class " + clazz.getName(), e);
        }
    }

    public static <T> T fromBytes(byte[] bytes, Class<T> clazz) {
        try {
            return MAPPER.readValue(bytes, clazz);
        } catch (Exception e) {
            throw new RuntimeException("JSON deserialization from bytes failed", e);
        }
    }

    public static byte[] toBytes(Object obj) {
        try {
            return MAPPER.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON serialization to bytes failed", e);
        }
    }
}
