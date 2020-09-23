package org.example.fraud.accumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public class BigDecimalCounter implements SimpleAccumulator<BigDecimal> {

    private BigDecimal localValue = BigDecimal.ZERO;

    private static final long serialVersionUID = 1L;

    public BigDecimalCounter() {
    }

    @Override
    public void add(BigDecimal value) {
        localValue = localValue.add(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        return localValue;
    }

    @Override
    public void resetLocal() {
        this.localValue = BigDecimal.ZERO;
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        localValue = localValue.add(other.getLocalValue());
    }

    @Override
    public Accumulator<BigDecimal, BigDecimal> clone() {
        BigDecimalCounter result = new BigDecimalCounter();
        result.localValue = localValue;
        return result;
    }
}
