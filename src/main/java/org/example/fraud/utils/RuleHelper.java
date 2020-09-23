package org.example.fraud.utils;

import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.example.fraud.accumulators.AverageAccumulator;
import org.example.fraud.accumulators.BigDecimalCounter;
import org.example.fraud.accumulators.BigDecimalMaximum;
import org.example.fraud.accumulators.BigDecimalMinimum;
import org.example.fraud.domain.Rule;

import java.math.BigDecimal;

public class RuleHelper {

    public static SimpleAccumulator<BigDecimal> getAggregator(Rule rule) {
        switch (rule.getAggregateFunctionType()) {
            case SUM:
                return new BigDecimalCounter();
            case AVG:
                return new AverageAccumulator();
            case MAX:
                return new BigDecimalMaximum();
            case MIN:
                return new BigDecimalMinimum();
            default:
                throw new RuntimeException("Unsupported aggregation function type: " + rule.getAggregateFunctionType());
        }
    }
}
