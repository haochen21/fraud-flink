/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.fraud;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.fraud.domain.Alert;
import org.example.fraud.domain.Rule;
import org.example.fraud.domain.Transaction;
import org.example.fraud.functions.AverageAggregate;
import org.example.fraud.functions.DynamicAlertFunction;
import org.example.fraud.functions.DynamicKeyFunction;
import org.example.fraud.sinks.LatencySink;
import org.example.fraud.sources.RulesSource;
import org.example.fraud.sources.TransactionsSource;
import org.example.fraud.utils.Descriptors;

import java.io.IOException;
import java.time.Duration;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public void run() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Rule> rulesUpdateStream = getRulesUpdateStream(env);
        DataStream<Transaction> transactions = getTransactionsStream(env);

        BroadcastStream<Rule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

        DataStream<Alert> alerts = transactions
                .connect(rulesStream)
                .process(new DynamicKeyFunction())
                .uid("DynamicKeyFunction")
                .name("Dynamic Partitioning Function")
                .keyBy((keyed) -> keyed.getKey())
                .connect(rulesStream)
                .process(new DynamicAlertFunction())
                .uid("DynamicAlertFunction")
                .name("Dynamic Rule Evaluation Function");

        DataStream<String> allRuleEvaluations =
                ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.demoSinkTag);

        DataStream<Long> latency =
                ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.latencySinkTag);

        DataStream<Rule> currentRules =
                ((SingleOutputStreamOperator<Alert>) alerts).getSideOutput(Descriptors.currentRulesSinkTag);

        alerts.print();

        DataStream<String> latencies = latency.timeWindowAll(Time.seconds(30))
                .aggregate(new AverageAggregate())
                .map(String::valueOf);
        latencies.addSink(LatencySink.createLatencySink());

        env.execute("Flink Streaming Java API Skeleton");
    }

    private DataStream<Transaction> getTransactionsStream(StreamExecutionEnvironment env) {
        SourceFunction<String> transactionSource = TransactionsSource.createTransactionsSource();
        DataStream<String> transactionsStringsStream =
                env.addSource(transactionSource)
                        .name("Transactions Source")
                        .setParallelism(1);
        DataStream<Transaction> transactionsStream =
                TransactionsSource.stringsStreamToTransactions(transactionsStringsStream);
        return transactionsStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofMillis(500))
                        .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                        .withIdleness(Duration.ofMillis(500)));
    }

    private DataStream<Rule> getRulesUpdateStream(StreamExecutionEnvironment env) throws IOException {
        SourceFunction<String> rulesSource = RulesSource.createRulesSource();
        DataStream<String> rulesStrings =
                env.addSource(rulesSource).name("kafka").setParallelism(1);
        return RulesSource.stringsStreamToRules(rulesStrings);
    }

    public static void main(String[] args) throws Exception {
        StreamingJob streamingJob = new StreamingJob();
        streamingJob.run();
    }
}
