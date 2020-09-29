package org.example.fraud.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.fraud.utils.JsonMapper;

@Slf4j
public class JsonSerializer<T> extends RichFlatMapFunction<T, String> {

    private JsonMapper<T> parser;

    private final Class<T> targetClass;

    public JsonSerializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parser = new JsonMapper<>(targetClass);
    }

    @Override
    public void flatMap(T value, Collector<String> out) throws Exception {
        try {
            String serialized = parser.toString(value);
            out.collect(serialized);
        } catch (Exception ex) {
            log.error("Failed serializing to JSON, dropping it: ", ex);
        }
    }
}
