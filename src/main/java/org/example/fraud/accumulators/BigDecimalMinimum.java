package org.example.fraud.accumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public class BigDecimalMinimum implements SimpleAccumulator<BigDecimal> {

    private BigDecimal min = BigDecimal.valueOf(Double.MAX_VALUE);

    private final BigDecimal limit = BigDecimal.valueOf(Double.MAX_VALUE);

    private static final long serialVersionUID = 1L;

    public BigDecimalMinimum() {
    }

    @Override
    public void add(BigDecimal value) {
        if (value.compareTo(limit) > 0) {
            throw new IllegalArgumentException("only support values less than Double.MAX_VALUE");
        }
        this.min = min.min(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        return min;
    }

    @Override
    public void resetLocal() {
        this.min = BigDecimal.valueOf(Double.MAX_VALUE);
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        this.min = min.min(other.getLocalValue());
    }

    @Override
    public Accumulator<BigDecimal, BigDecimal> clone() {
        BigDecimalMinimum result = new BigDecimalMinimum();
        result.min = this.min;
        return result;
    }
}
