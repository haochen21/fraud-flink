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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.fraud.domain.Alert;
import org.example.fraud.domain.Rule;
import org.example.fraud.domain.Transaction;
import org.example.fraud.functions.AverageAggregate;
import org.example.fraud.functions.DynamicAlertFunction;
import org.example.fraud.functions.DynamicKeyFunction;
import org.example.fraud.functions.JsonSerializer;
import org.example.fraud.params.Config;
import org.example.fraud.params.Parameters;
import org.example.fraud.sinks.LatencySink;
import org.example.fraud.sources.RulesSource;
import org.example.fraud.sources.TransactionsSource;
import org.example.fraud.utils.Descriptors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;

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
@Slf4j
public class StreamingJob {

    private Config config;

    public StreamingJob(Config config) {
        this.config = config;
    }

    public void run() throws Exception {

        final StreamExecutionEnvironment env = configureStreamExecutionEnvironment();

        Optional<Path> lastCheckpointPath = getLastCheckPoint();
        if (lastCheckpointPath.isPresent()) {
            String filePath = lastCheckpointPath.get().toAbsolutePath().toString();
            config.put(Parameters.lastCheckpointFilePath, filePath);
            String param = "--last-checkpoint file://" + filePath;
            ParameterTool parameterTool = ParameterTool.fromArgs(new String[]{"--last-checkpoint", "file://" + filePath});
            env.getConfig().setGlobalJobParameters(parameterTool);
        }


        DataStream<Rule> rulesUpdateStream = getRulesUpdateStream(env, config);
        DataStream<Transaction> transactions = getTransactionsStream(env, config);

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

        alerts.print().name("Alert STOUT SINK");

        allRuleEvaluations.print().name("Rule Evaluation Sink");

        DataStream<String> currentRulesJson = currentRules.flatMap(new JsonSerializer<>(Rule.class)).name("Rules Deserialzation");
        currentRulesJson.print();

        DataStream<String> latencies = latency.timeWindowAll(Time.seconds(30))
                .aggregate(new AverageAggregate())
                .map(String::valueOf);
        latencies.addSink(LatencySink.createLatencySink());

        env.execute("Flink Streaming Java API Skeleton");
    }

    private StreamExecutionEnvironment configureStreamExecutionEnvironment() {
        Configuration flinkConfig = new Configuration();

        boolean isLocal = config.get(Parameters.LOCAL_EXECUTION);
        StreamExecutionEnvironment env =
                isLocal
                        ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
                        : StreamExecutionEnvironment.createLocalEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(config.get(Parameters.SOURCE_PARALLELISM));

        boolean enableCheckpoints = config.get(Parameters.ENABLE_CHECKPOINTS);
        if (enableCheckpoints) {
            env.enableCheckpointing(config.get(Parameters.CHECKPOINT_INTERVAL));
            //env.getCheckpointConfig()
                    //.setMinPauseBetweenCheckpoints(config.get(Parameters.MIN_PAUSE_BTWN_CHECKPOINTS));
            // cancel是是否需要保留当前的checkpoint, 默认checkpoint会在整个作业cancel时被删除，checkpoint是作业级别的保存点
            env.getCheckpointConfig()
                    .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.setStateBackend(new FsStateBackend("file://" + config.get(Parameters.CHECKPOINT_DATA_DIR)));
        }

        return env;
    }

    private DataStream<Transaction> getTransactionsStream(StreamExecutionEnvironment env, Config config) {
        SourceFunction<String> transactionSource = TransactionsSource.createTransactionsSource(config);
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

    private DataStream<Rule> getRulesUpdateStream(StreamExecutionEnvironment env, Config config) throws IOException {
        SourceFunction<String> rulesSource = RulesSource.createRulesSource(config);
        DataStream<String> rulesStrings =
                env.addSource(rulesSource).name("kafka").setParallelism(1);
        return RulesSource.stringsStreamToRules(rulesStrings);
    }

    private Optional<Path> getLastCheckPoint() throws IOException {
        Optional<Path> dirPath = Files.list(Paths.get(config.get(Parameters.CHECKPOINT_DATA_DIR)))
                .filter(p -> Files.isDirectory(p))
                .sorted((p1, p2) -> Long.valueOf(p2.toFile().lastModified())
                        .compareTo(p1.toFile().lastModified()))
                .findFirst();
        if (dirPath.isPresent()) {
            log.info("last checkpoint dir: " + dirPath.get().toString());
            Optional<Path> chkDir = Files.list(dirPath.get())
                    .filter(p -> p.getFileName().toString().startsWith("chk-"))
                    .sorted((p1, p2) -> Long.valueOf(p2.toFile().lastModified())
                            .compareTo(p1.toFile().lastModified()))
                    .findFirst();
            if (chkDir.isPresent()) {
                log.info("chk dir is: " + chkDir.get().toString());
            }
            return chkDir;
        }
        return Optional.empty();
    }

    public static void main(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(
                StreamingJob.class.getClassLoader().getResourceAsStream("fraud.properties"));
        Parameters inputParams = new Parameters(tool);
        Config config = new Config(inputParams, Parameters.STRING_PARAMS, Parameters.INTEGER_PARAMS, Parameters.BOOLEAN_PARAMS);

        StreamingJob streamingJob = new StreamingJob(config);
        streamingJob.run();
    }
}
