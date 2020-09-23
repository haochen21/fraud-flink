package org.example.fraud.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Transaction implements TimestampAssignable<Long> {

    public long transactionId;

    public long eventTime;

    public long payeeId;

    public long beneficiaryId;

    public BigDecimal paymentAmount;

    public PaymentType paymentType;

    private Long ingestionTimestamp;

    public enum PaymentType {

        WEIXINPAY("WEIXINPAY"), ALIPAY("ALIPAY");

        String representation;

        PaymentType(String representation) {
            this.representation = representation;
        }
    }

    @Override
    public void assignIngestionTimestamp(Long timestamp) {
        this.ingestionTimestamp = timestamp;
    }
}
