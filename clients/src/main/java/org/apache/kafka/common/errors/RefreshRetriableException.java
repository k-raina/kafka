package org.apache.kafka.common.errors;

public abstract class RefreshRetriableException extends RetriableException {
    public RefreshRetriableException(String message, Throwable cause) {
        super(message, cause);
    }

    public RefreshRetriableException(String message) {
        super(message);
    }

    public RefreshRetriableException(Throwable cause) {
        super(cause);
    }

    public RefreshRetriableException() {
    }
}
