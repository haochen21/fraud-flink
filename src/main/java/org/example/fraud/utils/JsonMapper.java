package org.example.fraud.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonMapper<T> {

    private final Class<T> targetClass;

    private final ObjectMapper objectMapper;

    public JsonMapper(Class<T> targetClass) {
        this.targetClass = targetClass;
        objectMapper = new ObjectMapper();
    }

    public T fromString(String line) throws IOException {
        return objectMapper.readValue(line, targetClass);
    }

    public String toString(T object) throws IOException {
        return objectMapper.writeValueAsString(object);
    }
}
