package org.example.fraud.params;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Param<T> {

    private String name;

    private Class<T> type;

    private T defaultValue;

    public static Param<String> string(String name, String defaultValue) {
        return new Param<>(name, String.class, defaultValue);
    }

    public static Param<Integer> integer(String name, Integer defaultValue) {
        return new Param<>(name, Integer.class, defaultValue);
    }

    public static Param<Boolean> bool(String name, Boolean defaultValue) {
        return new Param<>(name, Boolean.class, defaultValue);
    }
}
