package org.example.fraud.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.fraud.utils.JsonMapper;

@Slf4j
public class JsonDeserializer<T> extends RichFlatMapFunction<String, T> {

    private JsonMapper<T> parser;

    private final Class<T> targetClass;

    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parser = new JsonMapper<>(targetClass);
    }

    @Override
    public void flatMap(String value, Collector<T> out) throws Exception {
        log.info("{}", value);
        try {
            T parsed = parser.fromString(value);
            out.collect(parsed);
        } catch (Exception ex) {
            log.error("Failed parsing rule, dropping it,", ex);
        }
    }
}
