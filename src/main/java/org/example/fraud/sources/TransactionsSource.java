package org.example.fraud.sources;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.fraud.domain.Transaction;
import org.example.fraud.functions.JsonDeserializer;
import org.example.fraud.functions.TimeStamper;

import java.util.Properties;

public class TransactionsSource {

    public static SourceFunction<String> createTransactionsSource() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.31.39:9095,192.168.31.39:9096");
        properties.setProperty("group.id", "transactions");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("transactions", new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();
        return kafkaConsumer;
    }

    public static DataStream<Transaction> stringsStreamToTransactions(
            DataStream<String> transactionStrings) {
        return transactionStrings
                .flatMap(new JsonDeserializer<>(Transaction.class))
                .returns(Transaction.class)
                .flatMap(new TimeStamper<Transaction>())
                .returns(Transaction.class)
                .name("Transactions Deserialization");
    }
}
