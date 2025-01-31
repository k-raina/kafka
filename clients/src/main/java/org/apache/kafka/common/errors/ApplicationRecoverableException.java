package org.apache.kafka.common.errors;

public abstract class ApplicationRecoverableException extends ApiException {
    public ApplicationRecoverableException(String message) {
        super(message);
    }
}
