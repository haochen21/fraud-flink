package org.example.fraud.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.flink.api.common.time.Time;

import java.math.BigDecimal;
import java.util.List;

@Data
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Rule {

    @EqualsAndHashCode.Include
    private Integer ruleId;

    private RuleState ruleState;

    private List<String> groupingKeyNames;

    private List<String> unique;

    private String aggregateFieldName;

    private AggregateFunctionType aggregateFunctionType;

    private LimitOperatorType limitOperatorType;

    private BigDecimal limit;

    private Integer windowMinutes;

    private ControlType controlType;

    public long getWindowMills() {
        return Time.minutes(this.windowMinutes).toMilliseconds();
    }

    public long getWindowStartFor(Long timestamp) {
        Long ruleWindowMills = getWindowMills();
        return timestamp - ruleWindowMills;
    }

    public boolean apply(BigDecimal comparisonValue) {
        switch (limitOperatorType) {
            case EQUAL:
                return comparisonValue.compareTo(limit) == 0;
            case NOT_EQUAL:
                return comparisonValue.compareTo(limit) != 0;
            case GREATER:
                return comparisonValue.compareTo(limit) > 0;
            case GREATER_EQUAL:
                return comparisonValue.compareTo(limit) >= 0;
            case LESS:
                return comparisonValue.compareTo(limit) < 0;
            case LESS_EQUAL:
                return comparisonValue.compareTo(limit) <= 0;
            default:
                throw new RuntimeException("Unknown limit operator type: " + limitOperatorType);
        }
    }

    public enum RuleState {
        ACTIVE, PAUSE, DELETE, CONTROL;
    }

    public enum ControlType {
        CLEAR_STATE_ALL, CLEAR_STATE_ALL_STOP, DELETE_RULES_ALL, EXPORT_RULES_CURRENT;
    }

    public enum AggregateFunctionType {
        SUM, AVG, MIN, MAX;
    }

    public enum LimitOperatorType {

        EQUAL("="), NOT_EQUAL("!="), GREATER_EQUAL(">="), LESS_EQUAL("<="), GREATER(">"), LESS("<");

        String operator;

        LimitOperatorType(String operator) {
            this.operator = operator;
        }

        public static LimitOperatorType fromOperatorName(String operatorName) {
            for (LimitOperatorType limitOperatorType : LimitOperatorType.values()) {
                if (limitOperatorType.operator.equals(operatorName)) {
                    return limitOperatorType;
                }
            }
            return null;
        }
    }
}