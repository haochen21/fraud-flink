package org.apache.flink.streaming.api.environment;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.graph.StreamGraph;

import javax.annotation.Nonnull;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Slf4j
@Public
public class LocalStreamEnvironment extends StreamExecutionEnvironment {

    private static final String LAST_CHECKPOINT = "last-checkpoint";

    /**
     * Creates a new mini cluster stream environment that uses the default configuration.
     */
    public LocalStreamEnvironment() {
        this(new Configuration());
    }

    /**
     * Creates a new mini cluster stream environment that configures its local executor with the given configuration.
     *
     * @param configuration The configuration used to configure the local executor.
     */
    public LocalStreamEnvironment(@Nonnull Configuration configuration) {
        super(validateAndGetConfiguration(configuration));
        setParallelism(1);
    }

    private static Configuration validateAndGetConfiguration(final Configuration configuration) {
        if (!ExecutionEnvironment.areExplicitEnvironmentsAllowed()) {
            throw new InvalidProgramException(
                    "The LocalStreamEnvironment cannot be used when submitting a program through a client, " +
                            "or running in a TestEnvironment context.");
        }
        final Configuration effectiveConfiguration = new Configuration(checkNotNull(configuration));
        effectiveConfiguration.set(DeploymentOptions.TARGET, "local");
        effectiveConfiguration.set(DeploymentOptions.ATTACHED, true);
        return effectiveConfiguration;
    }

    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
        // transform the streaming program into a JobGraph
        StreamGraph streamGraph = getStreamGraph();
        streamGraph.setJobName(jobName);

        JobGraph jobGraph = streamGraph.getJobGraph();

        // ##############################################################################
        // 获取全局Job参数
        Map<String, String> parameters = this.getConfig().getGlobalJobParameters().toMap();
        if (parameters.containsKey(LAST_CHECKPOINT)) {
            // 加载checkpoint
            String checkpointPath = parameters.get(LAST_CHECKPOINT);
            jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(checkpointPath));
        }
        // ##############################################################################

        Configuration configuration = new Configuration();
        configuration.addAll(jobGraph.getJobConfiguration());

        // add (and override) the settings with what the user defined
        configuration.addAll(this.getConfiguration());

        if (!configuration.contains(RestOptions.BIND_PORT)) {
            configuration.setString(RestOptions.BIND_PORT, "0");
        }

        int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, jobGraph.getMaximumParallelism());

        MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                .build();

        MiniCluster miniCluster = new MiniCluster(cfg);

        try {
            miniCluster.start();
            configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().get().getPort());

            return miniCluster.executeJobBlocking(jobGraph);
        } finally {
            transformations.clear();
            miniCluster.close();
        }
    }
}
