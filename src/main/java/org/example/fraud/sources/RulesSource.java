package org.example.fraud.sources;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.fraud.domain.Rule;
import org.example.fraud.functions.JsonDeserializer;

import java.time.Duration;
import java.util.Properties;

public class RulesSource {

    private static final int RULES_STREAM_PARALLELISM = 1;

    public static SourceFunction<String> createRulesSource() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.31.39:9095,192.168.31.39:9096");
        properties.setProperty("group.id", "rules");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("rules", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        return kafkaConsumer;
    }

    public static DataStream<Rule> stringsStreamToRules(DataStream<String> ruleStrings) {
        return ruleStrings
                .flatMap(new JsonDeserializer<>(Rule.class))
                .returns(Rule.class)
                .name("Rule Deserialization")
                .setParallelism(RULES_STREAM_PARALLELISM)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Rule>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner((event, timestamp) -> Long.MAX_VALUE));
    }
}
