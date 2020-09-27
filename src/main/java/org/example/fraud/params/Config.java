package org.example.fraud.params;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Config {

    private final Map<Param<?>, Object> values = new HashMap<>();

    public <T> void put(Param<T> key, T value) {
        values.put(key, value);
    }

    public <T> T get(Param<T> key) {
        return key.getType().cast(values.get(key));
    }

    public <T> Config(Parameters inputParams, List<Param<String>> stringParams,
                      List<Param<Integer>> intParams, List<Param<Boolean>> boolParams) {
        overrideDefaultValue(inputParams, stringParams);
        overrideDefaultValue(inputParams, intParams);
        overrideDefaultValue(inputParams, boolParams);
    }

    private <T> void overrideDefaultValue(Parameters inputParams, List<Param<T>> params) {
        for (Param<T> param : params) {
            put(param, inputParams.getOrDefault(param));
        }
    }
}

