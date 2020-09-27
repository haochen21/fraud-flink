package org.example.fraud.sources;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.fraud.domain.Rule;
import org.example.fraud.functions.JsonDeserializer;
import org.example.fraud.params.Config;
import org.example.fraud.params.Parameters;

import java.time.Duration;
import java.util.Properties;

public class RulesSource {

    private static final int RULES_STREAM_PARALLELISM = 1;

    public static SourceFunction<String> createRulesSource(Config config) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", config.get(Parameters.KAFKA_HOST));
        properties.setProperty("group.id", config.get(Parameters.RULE_TOPIC));

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(config.get(Parameters.RULE_TOPIC), new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromLatest();
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
