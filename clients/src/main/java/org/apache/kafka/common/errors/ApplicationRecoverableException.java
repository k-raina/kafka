package org.apache.kafka.common.errors;

public abstract class ApplicationRecoverableException extends ApiException {
     public ApplicationRecoverableException(String message, Throwable cause) {
        super(message, cause);
    }

    public ApplicationRecoverableException(String message) {
        super(message);
    }
}
