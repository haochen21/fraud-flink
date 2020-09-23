package org.example.fraud.accumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public class BigDecimalMaximum implements SimpleAccumulator<BigDecimal> {

    private BigDecimal max = BigDecimal.valueOf(Double.MIN_VALUE);

    private final BigDecimal limit = BigDecimal.valueOf(Double.MIN_VALUE);

    private static final long serialVersionUID = 1L;

    public BigDecimalMaximum() {
    }

    @Override
    public void add(BigDecimal value) {
        if (value.compareTo(limit) < 0) {
            throw new IllegalArgumentException("only support values greater than Double.MIN_VALUE");
        }
        this.max = max.max(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        return max;
    }

    @Override
    public void resetLocal() {
        this.max = BigDecimal.valueOf(Double.MIN_VALUE);
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        this.max = max.max(other.getLocalValue());
    }

    @Override
    public Accumulator<BigDecimal, BigDecimal> clone() {
        BigDecimalMaximum result = new BigDecimalMaximum();
        result.max = this.max;
        return result;
    }
}
