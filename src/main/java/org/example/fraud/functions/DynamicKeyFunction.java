package org.example.fraud.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.fraud.domain.Keyed;
import org.example.fraud.domain.Rule;
import org.example.fraud.domain.Transaction;
import org.example.fraud.utils.Descriptors;
import org.example.fraud.utils.KeysExtractor;

import java.util.Iterator;
import java.util.Map;

/**
 * 根据规则动态的生成主键（数据分区）
 */
@Slf4j
public class DynamicKeyFunction
        extends BroadcastProcessFunction<Transaction, Rule, Keyed<Transaction, String, Integer>> {

    private RuleCounterGauge ruleCounterGauge;

    @Override
    public void open(Configuration parameters) throws Exception {
        ruleCounterGauge = new RuleCounterGauge();
        getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
    }

    @Override
    public void processElement(
            Transaction event, ReadOnlyContext ctx, Collector<Keyed<Transaction, String, Integer>> out)
            throws Exception {
        ReadOnlyBroadcastState<Integer, Rule> rulesState =
                ctx.getBroadcastState(Descriptors.rulesDescriptor);
        // 生成分区键
        int ruleCounter = 0;
        for (Map.Entry<Integer, Rule> entry : rulesState.immutableEntries()) {
            final Rule rule = entry.getValue();
            // 事件，主键，规则ID
            Keyed<Transaction, String, Integer> keyed =
                    new Keyed<>(event, KeysExtractor.getKey(rule.getGroupingKeyNames(), event), rule.getRuleId());
            ruleCounter++;
        }
        ruleCounterGauge.setValue(ruleCounter);
    }

    @Override
    public void processBroadcastElement(
            Rule rule, Context ctx, Collector<Keyed<Transaction, String, Integer>> out)
            throws Exception {
        log.info("{}", rule);
        BroadcastState<Integer, Rule> broadcastState = ctx.getBroadcastState(Descriptors.rulesDescriptor);
        switch (rule.getRuleState()) {
            case ACTIVE:
            case PAUSE:
                broadcastState.put(rule.getRuleId(), rule);
                break;
            case DELETE:
                broadcastState.remove(rule.getRuleId());
                break;
            case CONTROL:
                switch (rule.getControlType()) {
                    case DELETE_RULES_ALL:
                        Iterator<Map.Entry<Integer, Rule>> entryIterator = broadcastState.iterator();
                        while (entryIterator.hasNext()) {
                            Map.Entry<Integer, Rule> ruleEntry = entryIterator.next();
                            broadcastState.remove(ruleEntry.getKey());
                            log.info("Removed Rule {}", ruleEntry.getKey());
                        }
                        break;
                }
                break;
        }

    }

    private static class RuleCounterGauge implements Gauge<Integer> {

        private int value = 0;

        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public Integer getValue() {
            return value;
        }
    }
}
