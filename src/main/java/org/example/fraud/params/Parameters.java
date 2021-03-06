package org.example.fraud.params;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Arrays;
import java.util.List;

public class Parameters {

    private final ParameterTool tool;

    public Parameters(ParameterTool tool) {
        this.tool = tool;
    }

    public <T> T getOrDefault(Param<T> param) {
        if (!tool.has(param.getName())) {
            return param.getDefaultValue();
        }
        Object value;
        if (param.getType() == Integer.class) {
            value = tool.getInt(param.getName());
        } else if (param.getType() == Long.class) {
            value = tool.getLong(param.getName());
        } else if (param.getType() == Double.class) {
            value = tool.getDouble(param.getName());
        } else if (param.getType() == Boolean.class) {
            value = tool.getBoolean(param.getName());
        } else {
            value = tool.get(param.getName());
        }

        return param.getType().cast(value);
    }

    // kafka
    public static final Param<String> KAFKA_HOST = Param.string("kafka.bootstrapAddress", "localhost:9092");

    public static final Param<String> DATA_TOPIC = Param.string("kafka.topic.transactions", "livetransactions");
    public static final Param<String> RULE_TOPIC = Param.string("kafka.topic.rules", "rules");
    public static final Param<String> ALERT_TOPIC = Param.string("kafka.topic.alerts", "alerts");

    public static final Param<Integer> SOURCE_PARALLELISM = Param.integer("source.parallelism", 2);
    public static final Param<Boolean> ENABLE_CHECKPOINTS = Param.bool("enable.checkpoints", false);
    public static final Param<Boolean> LOCAL_EXECUTION = Param.bool("local.execution", false);
    public static final Param<Integer> CHECKPOINT_INTERVAL = Param.integer("checkpoint.interval", 60 * 1000);
    public static final Param<Integer> MIN_PAUSE_BTWN_CHECKPOINTS = Param.integer("min.pause.btwn.checkpoints", 600 * 1000);
    public static final Param<String> CHECKPOINT_DATA_DIR = Param.string("checkpoint.data.dir", "/Users/chenhao/Documents/fraud_checkpoint");
    public static final Param<String> lastCheckpointFilePath = Param.string("last.checkpoint.filePath", "");

    public static final List<Param<String>> STRING_PARAMS =
            Arrays.asList(
                    KAFKA_HOST,
                    DATA_TOPIC,
                    RULE_TOPIC,
                    ALERT_TOPIC,
                    CHECKPOINT_DATA_DIR,
                    lastCheckpointFilePath
            );

    public static final List<Param<Integer>> INTEGER_PARAMS =
            Arrays.asList(
                    SOURCE_PARALLELISM,
                    CHECKPOINT_INTERVAL,
                    MIN_PAUSE_BTWN_CHECKPOINTS
            );

    public static final List<Param<Boolean>> BOOLEAN_PARAMS =
            Arrays.asList(
                    ENABLE_CHECKPOINTS,
                    LOCAL_EXECUTION
            );

}
