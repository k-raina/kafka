package org.apache.kafka.common.errors;

public abstract class RefreshRetriableException extends RetriableException {
    public RefreshRetriableException(String message) {
        super(message);
    }
}
