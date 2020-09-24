package org.example.fraud.utils;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;
import org.example.fraud.domain.Rule;

public class Descriptors {

    public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
            new MapStateDescriptor<Integer, Rule>(
                    "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

    public static final OutputTag<Rule> currentRulesSinkTag =
            new OutputTag<Rule>("current-rules-sink") {
            };

    public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {
    };

    public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {
    };
}
