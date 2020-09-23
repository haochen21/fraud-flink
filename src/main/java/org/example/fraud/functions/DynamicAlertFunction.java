package org.example.fraud.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.example.fraud.domain.Alert;
import org.example.fraud.domain.Keyed;
import org.example.fraud.domain.Rule;
import org.example.fraud.domain.Transaction;
import org.example.fraud.utils.Descriptors;
import org.example.fraud.utils.FieldsExtractor;
import org.example.fraud.utils.RuleHelper;

import java.math.BigDecimal;
import java.util.*;

@Slf4j
public class DynamicAlertFunction
        extends KeyedBroadcastProcessFunction<String, Keyed<Transaction, String, Integer>, Rule, Alert> {

    private static final String COUNT = "COUNT_FLINK";

    private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

    private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;

    private static int CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;

    private transient MapState<Long, Set<Transaction>> windowsState;

    private Meter alertMeter;

    private MapStateDescriptor<Long, Set<Transaction>> windowStateDescriptor =
            new MapStateDescriptor<>(
                    "windowState",
                    BasicTypeInfo.LONG_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Set<Transaction>>() {
                    })
            );

    @Override
    public void open(Configuration parameters) throws Exception {
        windowsState = getRuntimeContext().getMapState(windowStateDescriptor);

        alertMeter = new MeterView(60);
        getRuntimeContext().getMetricGroup().meter("alertPerSecond", alertMeter);
    }

    @Override
    public void processElement(Keyed<Transaction, String, Integer> value, ReadOnlyContext ctx, Collector<Alert> out) throws Exception {
        long currentEventTime = value.getWrapped().getEventTime();

        Set<Transaction> valuesSet = windowsState.get(currentEventTime);
        if (valuesSet == null) {
            valuesSet = new HashSet<>();
        }
        valuesSet.add(value.getWrapped());
        windowsState.put(currentEventTime, valuesSet);

        long ingestionTime = value.getWrapped().getIngestionTimestamp();
        ctx.output(Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);

        Rule rule = ctx.getBroadcastState(Descriptors.rulesDescriptor).get(value.getId());
        if (Objects.isNull(rule)) {
            log.error("Rule with ID {} dose not exist", value.getId());
            return;
        }

        if (rule.getRuleState() == Rule.RuleState.ACTIVE) {
            // 窗口统计开始时间
            Long windowStartForEvent = rule.getWindowStartFor(currentEventTime);

            long cleanupTime = (currentEventTime / 1000) * 1000;
            ctx.timerService().registerEventTimeTimer(cleanupTime);

            SimpleAccumulator<BigDecimal> aggregator = RuleHelper.getAggregator(rule);
            for (Long stateEventTime : windowsState.keys()) {
                if (stateEventTime >= windowStartForEvent && stateEventTime <= currentEventTime) {
                    Set<Transaction> inWindow = windowsState.get(stateEventTime);
                    if (rule.getAggregateFieldName().equals(COUNT)
                            || rule.getAggregateFieldName().equals(COUNT_WITH_RESET)) {
                        for (Transaction transaction : inWindow) {
                            aggregator.add(BigDecimal.ONE);
                        }
                    } else {
                        for (Transaction transaction : inWindow) {
                            BigDecimal aggregateValue =
                                    FieldsExtractor.getBigDecimalByName(rule.getAggregateFieldName(), transaction);
                            aggregator.add(aggregateValue);
                        }
                    }
                }
            }
        }

    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Alert> out) throws Exception {
        log.info("{}", rule);
        BroadcastState<Integer, Rule> broadcastState =
                ctx.getBroadcastState(Descriptors.rulesDescriptor);

        switch (rule.getRuleState()) {
            case ACTIVE:
            case PAUSE:
                broadcastState.put(rule.getRuleId(), rule);
                break;
            case DELETE:
                broadcastState.remove(rule.getRuleId());
                break;
        }

        // update widest window rule
        if (rule.getRuleState() == Rule.RuleState.ACTIVE) {
            Rule widestWindowRule = broadcastState.get(WIDEST_RULE_KEY);
            if (Objects.isNull(widestWindowRule)) {
                broadcastState.put(WIDEST_RULE_KEY, rule);
            } else if (widestWindowRule.getWindowMills() < rule.getWindowMills()) {
                broadcastState.put(WIDEST_RULE_KEY, rule);
            }
        } else if (rule.getRuleState() == Rule.RuleState.CONTROL) {
            Rule.ControlType controlType = rule.getControlType();
            switch (controlType) {
                case EXPORT_RULES_CURRENT:
                    for (Map.Entry<Integer, Rule> entry : broadcastState.entries()) {
                        ctx.output(Descriptors.currentRulesSinkTag, entry.getValue());
                    }
                    break;
                case CLEAR_STATE_ALL:
                    ctx.applyToKeyedState(windowStateDescriptor, (key, state) -> state.clear());
                    break;
                case CLEAR_STATE_ALL_STOP:
                    broadcastState.remove(CLEAR_STATE_COMMAND_KEY);
                    break;
                case DELETE_RULES_ALL:
                    Iterator<Map.Entry<Integer, Rule>> entryIterator = broadcastState.iterator();
                    while (entryIterator.hasNext()) {
                        Map.Entry<Integer, Rule> ruleEntry = entryIterator.next();
                        broadcastState.remove(ruleEntry.getKey());
                        log.info("Removed Rule {}", ruleEntry.getKey());
                    }
                    break;
            }
        }
    }
}
