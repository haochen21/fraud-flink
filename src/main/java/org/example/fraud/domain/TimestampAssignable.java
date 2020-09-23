package org.example.fraud.domain;

public interface TimestampAssignable<T> {

    void assignIngestionTimestamp(T timestamp);
}
