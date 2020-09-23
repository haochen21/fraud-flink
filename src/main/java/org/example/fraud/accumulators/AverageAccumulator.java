package org.example.fraud.accumulators;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

import java.math.BigDecimal;

public class AverageAccumulator implements SimpleAccumulator<BigDecimal> {

    private long count;

    private BigDecimal sum;

    private static final long serialVersionUID = 1L;

    public AverageAccumulator() {
    }

    @Override
    public void add(BigDecimal value) {
        this.count++;
        this.sum = sum.add(value);
    }

    @Override
    public BigDecimal getLocalValue() {
        if(this.count == 0) {
            return BigDecimal.ZERO;
        }
        return this.sum.divide(new BigDecimal(count));
    }

    @Override
    public void resetLocal() {
        this.count = 0;
        this.sum = BigDecimal.ZERO;
    }

    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        AverageAccumulator avg = (AverageAccumulator) other;
        this.count += avg.count;
        this.sum = sum.add(avg.sum);
    }

    @Override
    public Accumulator<BigDecimal, BigDecimal> clone() {
        AverageAccumulator result = new AverageAccumulator();
        result.count = this.count;
        result.sum = this.sum;
        return result;
    }
}
